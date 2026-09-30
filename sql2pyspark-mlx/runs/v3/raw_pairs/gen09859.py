from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(F.col("age") == 66).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.end_date") <= "2024-10-01")
    .select(
        F.col("s.id"),
        F.col("s.monthly_fee"),
        F.col("s.customer_id"),
        F.col("s.end_date"),
    )
    .orderBy("monthly_fee", F.col("customer_id").desc())
)
