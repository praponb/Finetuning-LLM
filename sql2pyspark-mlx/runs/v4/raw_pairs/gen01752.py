from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") != "mobile") & ~F.col("country").isin(["JP", "DE"]) & (F.col("browser") == "Edge"))
    .select(
        "started_at",
        "country",
        "customer_id",
        F.upper("browser").alias("browser_upper"),
    )
)
