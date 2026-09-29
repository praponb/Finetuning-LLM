from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.channel") == "web")
    .groupBy(
        F.date_trunc("MONTH", F.col("p.launch_date")).alias("launch_month"),
        F.col("p.category"),
    )
    .agg(F.count("*").alias("num_order_items"))
    .filter(F.col("num_order_items") >= 3)
)
