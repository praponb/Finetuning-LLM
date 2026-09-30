from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter((F.col("s.country") == "UK") & ~F.col("s.device").isin(["desktop", "tablet", "mobile"]))
    .select(
        F.col("we.id"),
        F.col("we.page"),
        F.col("s.browser"),
        F.col("c.name"),
        F.col("c.credit_limit"),
    )
)
