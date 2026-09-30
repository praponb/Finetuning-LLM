from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("oi.unit_price") > 270)
    .groupBy(F.col("s.city"))
    .agg(
        F.sum(F.col("oi.quantity")).alias("total_quantity"),
        F.count("*").alias("row_count"),
    )
)
