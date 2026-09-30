from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Firefox", "Chrome"]))
    .select(
        "customer_id",
        "started_at",
        "device",
        F.initcap(F.trim("country")).alias("country_title"),
    )
)
