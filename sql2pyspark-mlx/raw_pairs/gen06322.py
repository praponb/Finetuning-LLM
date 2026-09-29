from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter((F.col("s.device") == "tablet") | F.col("c.is_active"))
    .select(
        F.col("c.credit_limit"),
        F.col("c.name"),
        F.col("c.email"),
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.concat_ws("-", F.col("s.device"), F.col("s.browser")).alias("device_browser_key"),
    )
)
