from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(~F.col("s.browser").isin(["Firefox", "Edge", "Chrome"]))
    .select(
        F.col("c.segment"),
        F.col("c.credit_limit"),
        F.col("c.signup_date"),
        F.col("s.started_at"),
    )
    .orderBy("started_at", "segment")
)
