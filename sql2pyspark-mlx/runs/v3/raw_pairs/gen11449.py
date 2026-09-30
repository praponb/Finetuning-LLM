from pyspark.sql import functions as F

customer_totals = (
    df_orders
    .groupBy("customer_id")
    .agg(F.min("discount").alias("min_discount"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(F.col("c.city"), F.col("t.min_discount"))
    .orderBy(F.col("city").desc())
)
