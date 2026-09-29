from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("s.plan") == "basic")
    .select(
        F.col("c.is_active"),
        F.col("c.city"),
        F.col("s.monthly_fee"),
        F.col("s.plan"),
    )
    .orderBy("monthly_fee", "plan")
)
