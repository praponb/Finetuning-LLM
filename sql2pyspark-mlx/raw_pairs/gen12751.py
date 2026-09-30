from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

result = (
    flattened
    .groupBy(
        F.when(F.col("status_code") >= 396, "HIGH").otherwise("LOW").alias("status_code_tier"),
    )
    .agg(
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles_2"),
        F.sum(F.when(F.col("host") == "h1", F.col("status_code")).otherwise(0)).alias("h1_status_code"),
    )
)
