from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.brand").isin(["Globex", "Acme", "Stark"]))
    .groupBy(F.col("p.category"))
    .agg(
        F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"),
        F.avg(F.col("oi.quantity")).alias("avg_quantity"),
    )
)
