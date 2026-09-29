from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

filtered = (
    lines
    .filter(F.col("latency_ms").between(1096, 1514))
    .select(
        "service",
        "status_code",
        "attr_key",
        F.length("attr_key").alias("attr_key_length"),
        F.trim("service").alias("clean_service"),
        F.coalesce("attr_key", F.lit("NONE")).alias("attr_key_clean"),
    )
)

base = (
    filtered
    .filter(F.col("attr_key_length") <= 38)
    .withColumn("status_code_rounded", F.round("status_code", 2))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("DIRECT")))
)

cleaned = (
    base
    .filter(F.col("status_code_rounded").between(54, 71))
    .select(
        "service",
        "status_code",
        "attr_key",
        "attr_key_length",
        "clean_service",
        "attr_key_filled",
        F.when(F.col("service") == "billing", 1).otherwise(0).alias("is_billing"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("status_code") >= 405, "LARGE").otherwise("SMALL").alias("status_code_tier"),
        F.when(F.col("is_billing") >= 63, "LARGE").otherwise("SMALL").alias("is_billing_tier"),
    )
    .agg(
        F.percentile_approx("attr_key_length", [0.25, 0.5, 0.75]).alias("attr_key_length_quartiles"),
        F.count("*").alias("event_count"),
        F.countDistinct("service").alias("distinct_services"),
    )
    .orderBy("event_count")
)
