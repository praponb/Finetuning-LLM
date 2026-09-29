from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at").desc(), F.col("id").desc())

filtered = (
    df_meter_readings
    .filter(F.col("kwh") > 45.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
        (F.col("read_at") + F.expr("INTERVAL 30 MINUTES")).alias("read_at_plus_30m"),
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix_2"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_activity = (
    latest.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")) & (F.col("m.voltage").between(220.7, 236.8)), "left_semi")
)

with_parent = (
    with_activity.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff") == "TOU"), "inner")
    .select("s.*", "si.site_name", F.col("si.zone").alias("site_zone"))
)

result = (
    with_parent
    .rollup("site_zone", "quality", "site_id")
    .agg(
        F.grouping("site_zone").alias("is_total_site_zone"),
        F.min("voltage").alias("min_voltage"),
        F.count(F.when(F.col("quality").isNotNull(), 1)).alias("n_quality_is_not_null"),
    )
    .select(
        F.coalesce(F.col("site_zone"), F.lit("ALL")).alias("site_zone"),
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "site_id",
        "is_total_site_zone",
        "min_voltage",
        "n_quality_is_not_null",
    )
    .orderBy(
        F.col("site_zone").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
