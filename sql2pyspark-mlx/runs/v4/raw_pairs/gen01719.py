from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("oi.quantity") < 6)
    .groupBy(F.col("p.supplier_id"))
    .agg(
        F.count("*").alias("num_order_items"),
        F.countDistinct(F.col("oi.quantity")).alias("unique_orders"),
    )
    .filter(F.col("num_order_items") >= 5)
)
