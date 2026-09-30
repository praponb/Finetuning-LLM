from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "log_ts",
        "status_code",
        "k",
        "attr_value",
        F.when(F.col("k").rlike("^(trace|version)$"), "GROUP_A").when(F.col("k").rlike("^(region)$"), "CORE").otherwise("PRIORITY").alias("k_zone"),
    )
    .orderBy(F.col("k_zone").desc_nulls_last())
)
