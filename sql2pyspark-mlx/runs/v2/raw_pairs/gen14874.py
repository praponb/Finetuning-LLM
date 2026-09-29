from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "mobile") & ~F.col("device").isin(["desktop", "tablet", "mobile"]) & F.col("customer_id").isNotNull())
    .select("id", "started_at")
    .orderBy("started_at", "id")
    .limit(5)
)
