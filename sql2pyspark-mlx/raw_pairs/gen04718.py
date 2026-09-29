from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.filter(F.col("age") == 54).alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left_semi")
    .filter(F.col("subscriptions.start_date") <= "2024-08-01")
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.plan"),
        F.col("subscriptions.status"),
    )
    .orderBy(F.col("plan").desc(), "id")
    .limit(10)
)
