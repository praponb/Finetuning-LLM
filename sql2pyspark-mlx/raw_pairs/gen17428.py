from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "status_code",
        "context",
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
        F.col("context")["region"].alias("region"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
        F.col("context").getItem("trace").alias("trace"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("status_code") >= 411, "HIGH").otherwise("LOW").alias("status_code_tier"),
    )
    .agg(
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count(F.when(F.col("level") == "WARN", 1)).alias("warn_count"),
        F.percentile_approx("status_code", 0.9).alias("status_code_p90"),
    )
    .filter(F.col("warn_count") > 1)
    .orderBy(F.col("status_code_p90").asc_nulls_first())
)
