from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(~((F.col("s.customer_id") == 35) | F.col("s.device").isin(["mobile", "tablet"])))
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("c.credit_limit"),
        F.concat_ws(",", F.col("s.country"), F.col("s.device")).alias("country_device_key"),
    )
)
