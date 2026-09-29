from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.name").like("H%"))
    .groupBy(F.quarter(F.col("c.signup_date")).alias("signup_quarter"))
    .agg(
        F.sum((F.col("oi.quantity") * F.col("oi.unit_price"))).alias("revenue"),
        F.count("*").alias("num_order_items"),
    )
)
