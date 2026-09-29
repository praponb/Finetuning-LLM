from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("order_id").isin([5, 17]))
    .groupBy("product_id")
    .agg(F.sum("unit_price").alias("total_unit_price"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.supplier_id"),
        F.col("ps.total_unit_price"),
    )
    .orderBy("supplier_id")
)
