from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("quality").orderBy(F.col("kwh").desc(), F.col("id").desc())

cleaned = (
    df_meter_readings
    .filter((F.col("site_id") >= 2) & (F.col("voltage").between(213.5, 234.7)))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.least(F.col("kwh"), F.col("voltage")).alias("least_kwh_voltage"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

qualified = (
    cleaned.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

qualified_2 = (
    qualified.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") < F.lit("2025-02-24 14:00:00").cast("timestamp")) & (F.col("m.voltage") <= 231.7), "left_anti")
)

deduped = (
    qualified_2
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .groupingSets(
        [["site_id", "quality_or_unknown"], ["site_id"], ["quality_or_unknown"], []],
        "site_id", "quality_or_unknown",
    )
    .agg(F.array_sort(F.collect_set("quality")).alias("quality_list"))
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("quality_or_unknown").asc_nulls_last(),
    )
)
