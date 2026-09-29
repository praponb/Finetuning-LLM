from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("c.city").isin(["London", "New York"]))
    .select(
        F.col("c.segment"),
        F.col("c.is_active"),
        F.col("c.email"),
        F.col("s.status"),
        F.col("s.customer_id"),
        F.col("s.plan"),
        (F.col("c.age") * F.col("c.credit_limit")).alias("age_times_credit_limit"),
    )
    .orderBy("customer_id")
)
