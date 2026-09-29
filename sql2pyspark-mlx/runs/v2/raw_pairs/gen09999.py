from pyspark.sql import functions as F

customer_totals = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.countDistinct("monthly_fee").alias("unique_customers"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.unique_customers") >= 10)
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("c.name"),
        F.col("t.unique_customers"),
    )
    .orderBy("country", "unique_customers")
)
