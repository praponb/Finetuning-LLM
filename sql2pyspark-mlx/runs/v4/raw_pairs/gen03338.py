from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .groupBy("customer_id")
    .agg(F.count("*").alias("num_orders"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.num_orders") >= 4)
    .select(F.col("c.id"), F.col("c.city"), F.col("cs.num_orders"))
    .orderBy(F.col("id").desc(), "city")
)
