from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

cleaned = (
    flattened
    .filter(F.col("key").isNotNull())
    .select("id", "level", "attr_value", F.trim("level").alias("clean_level"))
)

valid = (
    cleaned
    .filter(~F.col("level").isin(["WARN", "DEBUG", "INFO"]))
    .select(
        "*",
        F.trim("level").alias("clean_level_2"),
        F.when(F.col("level").isin(["INFO", "ERROR"]), "Group A").otherwise("Other").alias("level_group"),
    )
)

valid_app_logs = (
    valid
    .filter(F.col("level") == "INFO")
    .withColumn("level_title", F.initcap(F.trim("level")))
)

cleaned_app_logs = (
    valid_app_logs
    .filter(F.col("level") == "DEBUG")
    .select("*", F.when(F.col("level") == "ERROR", 1).otherwise(0).alias("is_error"))
)

valid_app_logs_2 = (
    cleaned_app_logs
    .filter(F.col("is_error") <= 45)
    .select(
        "attr_value",
        "clean_level",
        "clean_level_2",
        "level_group",
        "level_title",
        "is_error",
        F.upper("level").alias("level_upper"),
    )
)

report = valid_app_logs_2.select("level_group", "level_upper")

result = report.select("level_group")
