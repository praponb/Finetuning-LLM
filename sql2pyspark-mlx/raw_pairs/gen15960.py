from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("status_code") >= 450, "HIGH").otherwise("LOW").alias("status_code_tier"),
    )
    .agg(
        F.sum("status_code").alias("total_status_code"),
        F.sum(F.when(F.col("level") == "WARN", F.col("status_code")).otherwise(0)).alias("warn_status_code"),
    )
)
