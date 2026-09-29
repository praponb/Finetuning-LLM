from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.customer_id") == 11)
    .select(
        F.col("c.is_active"),
        F.col("c.age"),
        F.col("c.email"),
        F.col("s.monthly_fee"),
        F.col("s.monthly_fee").cast("int").alias("monthly_fee_int"),
    )
    .orderBy(F.col("age").desc())
)
