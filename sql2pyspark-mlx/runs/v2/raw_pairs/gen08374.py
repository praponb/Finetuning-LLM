from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("oi.quantity") > 6)
    .groupBy(F.col("p.supplier_id"))
    .agg(F.sum(F.col("oi.quantity")).alias("total_quantity"))
)
