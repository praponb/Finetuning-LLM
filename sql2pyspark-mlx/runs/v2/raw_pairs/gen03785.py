from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") >= "2025-01-14")
    .select(
        "customer_id",
        "started_at",
        F.initcap(F.trim("browser")).alias("browser_title"),
    )
)
