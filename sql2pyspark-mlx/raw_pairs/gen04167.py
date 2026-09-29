from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 4, 5]))
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.total_quantity") > 1300)
    .select(F.col("p.stock_qty"), F.col("ps.total_quantity"), F.col("ps.max_quantity"))
    .orderBy("total_quantity")
)
