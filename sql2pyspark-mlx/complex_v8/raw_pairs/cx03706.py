from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), F.col("id").desc())

prepared_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) | (F.col("read_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
    )
)

deduped = (
    prepared_readings
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

qualified = (
    deduped.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.length(F.trim(F.col("si.site_name"))) >= 5), "left_anti")
)

result = (
    qualified
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "greatest_kwh_voltage",
        F.abs(F.col("kwh") - 52.4).alias("kwh_distance"),
        F.round(F.col("voltage") * 0.25, 2).alias("voltage_adj"),
    )
    .orderBy(F.col("kwh_distance").asc())
)
