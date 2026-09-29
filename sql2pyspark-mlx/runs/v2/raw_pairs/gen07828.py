from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.filter(F.col("segment").isin(["SMB", "Corporate", "Consumer"])).alias("c"), F.col("o.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("o.order_date") <= "2025-05-01")
    .select(F.col("o.id"), F.col("o.order_date"))
)
