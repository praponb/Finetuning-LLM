from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter(F.col("site_id") >= 4)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("hour", F.col("read_at")).alias("read_at_hour"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

rollup_base = (
    base
    .groupBy("site_id", "quality")
    .agg(
        F.max("voltage").alias("max_voltage"),
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("quality").alias("distinct_qualitys"),
    )
    .filter(F.col("n_rows") >= 3)
)

with_activity = (
    rollup_base.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") < 236.7) & (F.col("m.quality") == "OK"), "left_anti")
)

result = (
    with_activity
    .filter(F.col("quality") != "OK")
    .select(
        "site_id",
        "quality",
        "max_voltage",
        "n_rows",
        "distinct_ids",
        "distinct_qualitys",
        F.when(F.col("distinct_qualitys") >= 7, "large").when(F.col("distinct_qualitys") >= 3, "medium").otherwise("small").alias("distinct_qualitys_tier"),
    )
    .orderBy(F.col("max_voltage").desc(), F.col("quality").desc_nulls_first())
)
