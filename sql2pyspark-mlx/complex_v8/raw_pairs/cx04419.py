from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), F.col("id").desc())

valid_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

deduped = (
    valid_readings
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

without_matches = (
    deduped.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage").between(216.5, 234.5)) & (F.month(F.col("m.read_at")).isin(1, 2)), "left_anti")
)

result = (
    without_matches
    .groupingSets(
        [["quality", "site_id"], ["quality"]],
        "quality", "site_id",
    )
    .agg(
        F.round(F.sum("voltage") / F.nullif(F.sum("kwh"), F.lit(0)), 3).alias("voltage_to_kwh_ratio"),
    )
    .select(
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "site_id",
        "voltage_to_kwh_ratio",
    )
    .orderBy(F.col("quality").asc_nulls_last(), F.col("site_id").asc_nulls_last())
)
