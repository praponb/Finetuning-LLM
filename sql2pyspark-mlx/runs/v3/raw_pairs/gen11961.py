from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.device").isin(["mobile", "tablet"]) | ~F.col("s.device").isin(["tablet", "desktop", "mobile"]))
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("c.segment"),
    )
    .orderBy(F.col("started_at").desc())
)
