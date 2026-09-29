from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .select(
        F.col("s.plan"),
        F.col("s.start_date"),
        F.col("c.segment"),
        F.when(F.col("c.city") == "Singapore", 1).otherwise(0).alias("is_singapore"),
    )
)
