from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("we.session_id"),
        F.col("s.country"),
        F.col("s.customer_id"),
        F.col("c.credit_limit"),
        F.when(F.col("s.country").isin(["TH", "UK"]), "Priority").otherwise("Other").alias("country_group"),
    )
    .orderBy("customer_id")
)
