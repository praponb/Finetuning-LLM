from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "status_code",
        "payload_json",
        "context",
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
        F.size("context").alias("num_context"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.coalesce(F.col("context").getItem("version"), F.lit("none")).alias("version_or_default"),
    )
)

result = (
    unpacked
    .withColumn("num_context_tier", F.when(F.col("num_context") >= 72, "HIGH").otherwise("LOW"))
    .rollup("num_context_tier")
    .agg(
        F.count_if(F.col("service") == "billing").alias("billing_events"),
        F.count(F.when(F.col("level") == "WARN", 1)).alias("warn_count"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.count("*").alias("event_count"),
        F.count_if(F.col("service") == "search").alias("search_events"),
        F.grouping("num_context_tier").alias("grouping_id"),
    )
    .filter(F.col("billing_events") > 1)
)
