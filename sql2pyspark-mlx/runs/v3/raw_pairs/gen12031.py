from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("started_at") < "2025-09-01") & (F.col("browser") != "Edge") & F.col("device").isin(["mobile", "desktop", "tablet"]))
    .select(
        "id",
        "customer_id",
        "browser",
        F.upper("browser").alias("browser_upper"),
        F.coalesce("browser", F.lit("N/A")).alias("browser_filled"),
    )
)
