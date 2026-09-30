from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "right")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.supplier_id").isin([4, 23]))
    .groupBy(
        F.col("p.brand"),
        F.quarter(F.col("p.launch_date")).alias("launch_quarter"),
    )
    .agg(
        F.sum((F.col("oi.quantity") * F.col("oi.unit_price"))).alias("revenue"),
        F.count("*").alias("order_item_count"),
    )
    .orderBy("brand", "launch_quarter")
    .limit(5)
)
