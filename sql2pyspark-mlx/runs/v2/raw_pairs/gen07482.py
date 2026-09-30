from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.is_active"), F.col("c.city"))
    .agg(
        F.count(F.col("p.amount")).alias("amount_count"),
        F.sum(F.when(F.col("p.method") == "card", 1).otherwise(0)).alias("card_count"),
    )
    .filter(F.col("card_count") >= 50)
    .orderBy(F.col("card_count").desc())
)
