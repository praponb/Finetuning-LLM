from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "outer")
    .groupBy(F.col("p.brand"))
    .agg(F.sum(F.col("oi.quantity")).alias("total_quantity"))
)
