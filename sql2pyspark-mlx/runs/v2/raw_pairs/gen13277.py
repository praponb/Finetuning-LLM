from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(F.col("c.name").like("Ol%") & F.col("o.channel").isin(["store", "mobile", "partner"]))
    .select(F.col("c.email"), F.col("o.store_id"), F.col("o.order_date"))
)
