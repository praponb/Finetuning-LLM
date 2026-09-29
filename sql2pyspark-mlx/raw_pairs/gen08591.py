from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key")

flattened = (
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

enriched = (
    flattened
    .filter(F.col("status_code") >= 291)
    .withColumn("key_max_status_code", F.max("status_code").over(w))
)

base = (
    enriched
    .filter((F.col("log_ts") > F.lit("2025-06-01 14:00:00").cast("timestamp")) & (F.col("service") == "billing"))
    .select(
        "id",
        "level",
        "attr_value",
        "key_max_status_code",
        F.trim("level").alias("clean_level"),
    )
)

base_app_logs = (
    base
    .filter(F.col("level") == "WARN")
    .select("id", "level", "clean_level", F.upper("level").alias("level_upper"))
)

filtered = (
    base_app_logs
    .filter(F.col("level").isin(["ERROR", "INFO", "WARN"]))
    .withColumn("level_lower", F.lower("level"))
)

result = (
    filtered
    .select(
        "id",
        "level",
        "clean_level",
        F.when(F.col("level").rlike("^(INFO|ERROR)$"), "CORE").otherwise("GROUP_A").alias("level_zone"),
    )
    .orderBy(F.col("level").asc_nulls_first())
)
