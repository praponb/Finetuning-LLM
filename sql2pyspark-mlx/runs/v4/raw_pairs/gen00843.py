from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

base = (
    flattened
    .filter(F.col("log_ts") >= F.to_date(F.lit("2025-06-01")))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "attr_value",
        F.trim("service").alias("clean_service"),
        F.lower("level").alias("level_lower"),
        F.coalesce("k", F.lit("NONE")).alias("k_clean"),
    )
)

valid = (
    base
    .filter((F.col("log_ts") > F.to_date(F.lit("2025-06-01"))) & F.col("service").isin(["billing", "checkout"]))
    .select(
        "id",
        "host",
        "level",
        "clean_service",
        "level_lower",
        F.lower("host").alias("host_lower"),
    )
)

valid_app_logs = (
    valid
    .filter(F.col("level").isin(["WARN", "ERROR", "DEBUG"]))
    .withColumn("host_level", F.concat("host", F.lit(" - "), "level"))
)

result = (
    valid_app_logs
    .select(
        "id",
        "host",
        "level",
        "clean_service",
        "host_lower",
        "host_level",
        F.when(F.col("host").rlike("^(h4|h2)$"), "CORE").otherwise("EXTENDED").alias("host_zone"),
    )
    .orderBy("id")
)
