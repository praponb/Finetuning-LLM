from pyspark.sql import functions as F

customer_totals = (
    df_orders
    .groupBy("customer_id")
    .agg(F.round(F.stddev("discount"), 2).alias("stddev_discount"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .filter(F.col("ct.stddev_discount") > 49)
    .select(F.col("c.id"), F.col("c.city"), F.col("ct.stddev_discount"))
    .orderBy("id", "city")
)
