from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("p.paid_at") < "2023-04-01")
    .groupBy(F.col("c.is_active"))
    .agg(
        F.sum(F.when(F.col("p.method") == "card", 1).otherwise(0)).alias("card_count"),
        F.count(F.col("p.amount")).alias("amount_count"),
    )
    .orderBy("card_count", "is_active")
    .limit(1)
)
