from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.col("host") != "h2")
    .select(
        "id",
        "service",
        "host",
        "level",
        "payload_json",
        "context",
        F.coalesce(F.col("context").getItem("version"), F.lit("none")).alias("version_or_default"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

result = (
    unpacked
    .withColumn("status_code_high_tier", F.when(F.col("status_code_high") >= 2, "PREMIUM").otherwise("STANDARD"))
    .rollup("status_code_high_tier")
    .agg(
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_rows"),
        F.count(F.when(F.col("host") == "h4", 1)).alias("h4_rows"),
        F.percentile_approx("status_code_high", [0.25, 0.5, 0.75]).alias("status_code_high_quartiles"),
        F.sum(F.when(F.col("host") == "h4", F.col("status_code_high")).otherwise(0)).alias("h4_status_code_high"),
        F.max(F.when(F.col("service") == "checkout", F.col("status_code_high"))).alias("checkout_max_status_code_high"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("status_code_high_tier").asc_nulls_last())
)
