from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 3, 6]))
    .groupBy("product_id")
    .agg(F.sum("revenue").alias("total_revenue"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .filter(F.col("pt.total_revenue") > 10000)
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("pt.total_revenue"))
)
