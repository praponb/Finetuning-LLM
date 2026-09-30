from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .filter(F.col("store_id") == 18)
    .groupBy("customer_id")
    .agg(F.min("discount").alias("min_discount"))
)

result = (
    df_customers.alias("c")
    .join(orders_by_customer.alias("O"), F.col("c.id") == F.col("O.customer_id"), "inner")
    .filter(F.col("O.min_discount") >= 0.39)
    .select(F.col("c.id"), F.col("c.city"), F.col("O.min_discount"))
    .orderBy(F.col("id").desc(), "min_discount")
)
