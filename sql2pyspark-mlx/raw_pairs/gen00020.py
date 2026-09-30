from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

filtered = (
    exploded
    .filter(F.col("key").isNotNull())
    .withColumn("key_prefix", F.substring("key", 1, 4))
    .withColumn("is_billing", F.when(F.col("service") == "billing", 1).otherwise(0))
)

valid = (
    filtered
    .filter((F.col("log_ts") >= F.lit("2025-06-01 06:00:00").cast("timestamp")) & F.col("key").isNotNull())
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "key",
        "attr_value",
        "key_prefix",
        "is_billing",
        F.trim("service").alias("clean_service"),
        F.least("status_code", "is_billing").alias("least_status_code_is_billing"),
    )
)

cleaned = (
    valid
    .filter((F.col("log_ts") > F.lit("2025-06-01 13:00:00").cast("timestamp")) & (F.col("least_status_code_is_billing") <= 48))
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "key",
        "attr_value",
        "key_prefix",
        "is_billing",
        "clean_service",
        "least_status_code_is_billing",
        (F.col("is_billing") * F.col("status_code")).alias("is_billing_times_status_code"),
        F.coalesce("key", F.lit("OTHER")).alias("key_filled"),
    )
)

result = (
    cleaned
    .groupBy(
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
        F.when(F.col("status_code") >= 401, "HIGH").otherwise("LOW").alias("status_code_tier"),
    )
    .agg(
        F.min("log_ts").alias("first_seen"),
        F.sum(F.when(F.col("key") == "version", F.col("is_billing_times_status_code"))).alias("version_total_is_billing_times_status_code"),
        F.count("*").alias("num_rows"),
    )
)
