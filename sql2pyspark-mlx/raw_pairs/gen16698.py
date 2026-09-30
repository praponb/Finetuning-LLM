from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        "level",
        F.when(F.col("duration_sec") >= 3.5, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
    )
    .agg(
        F.countDistinct("region_or_default").alias("distinct_region_or_defaults"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
    .orderBy(F.col("distinct_region_or_defaults").asc_nulls_first())
)
