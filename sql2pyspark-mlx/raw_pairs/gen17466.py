from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

cleaned = (
    flattened
    .filter(F.col("level") == "INFO")
    .select("*", F.trim("level").alias("clean_level"))
)

filtered = (
    cleaned
    .filter((~F.col("k").eqNullSafe(F.lit("version"))) & F.col("k").isNotNull())
    .select("*", F.trim("service").alias("clean_service"))
)

filtered_app_logs = (
    filtered
    .filter(F.col("k").isNotNull() & (~F.col("k").eqNullSafe(F.lit("region"))))
    .withColumn("level_group", F.when(F.col("level").isin(["INFO", "WARN"]), "Group A").otherwise("Other"))
    .withColumn("service_level", F.concat("service", F.lit("/"), "level"))
)

filtered_app_logs_2 = (
    filtered_app_logs
    .filter(F.col("k").eqNullSafe(F.lit("trace")))
    .select("*", F.concat_ws("-", "k", "service").alias("k_service_key"))
)

result = (
    filtered_app_logs_2
    .select(
        "service",
        "attr_value",
        "clean_level",
        "level_group",
        "service_level",
        "k_service_key",
        F.when(F.col("service").rlike("^(checkout|auth)$"), "PRIORITY").when(F.col("service").rlike("^(billing|search)$"), "CORE").otherwise("OTHER_ZONE").alias("service_zone"),
    )
    .orderBy(F.col("service").desc_nulls_last())
)
