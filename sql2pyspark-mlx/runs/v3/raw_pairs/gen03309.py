from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.when(F.col("c.age") >= 71, "high").when(F.col("c.age") >= 36, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.count(F.when(F.col("p.method") == "card", 1)).alias("card_count"))
    .filter(F.col("card_count") > 3)
)
