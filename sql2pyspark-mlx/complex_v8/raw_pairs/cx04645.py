from pyspark.sql import functions as F

valid = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

result = (
    valid
    .cube("quality", "site_id")
    .agg(
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.round(F.avg("kwh"), 2).alias("avg_kwh"),
    )
    .orderBy(F.col("quality").asc_nulls_last(), F.col("site_id").asc_nulls_last())
)
