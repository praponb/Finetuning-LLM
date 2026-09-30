from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.col("p.brand"),
        F.date_trunc("MONTH", F.col("p.launch_date")).alias("launch_month"),
    )
    .agg(F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"))
)
