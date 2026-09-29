from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "payload_json",
        "context",
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
        F.col("context").getItem("trace").alias("trace"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
    )
)

result = (
    flattened_attrs
    .groupBy("service")
    .agg(
        F.countDistinct("region_or_default").alias("distinct_region_or_defaults"),
        F.percentile_approx("has_fault", 0.5).alias("has_fault_p50"),
    )
    .filter(F.col("distinct_region_or_defaults") > 1)
    .orderBy(F.col("distinct_region_or_defaults").desc_nulls_first())
)
