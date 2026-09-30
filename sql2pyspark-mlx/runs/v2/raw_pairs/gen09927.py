from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .filter(F.col("amount") <= 1110)
    .groupBy("customer_id")
    .agg(
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.count("*").alias("order_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(orders_by_customer.alias("O"), F.col("c.id") == F.col("O.customer_id"), "inner")
    .filter(F.col("O.order_count") > 8)
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("c.age"),
        F.col("O.avg_discount"),
        F.col("O.order_count"),
    )
    .orderBy("order_count")
)
