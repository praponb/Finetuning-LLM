from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .filter(F.col("revenue") <= 3000)
    .groupBy("product_id")
    .agg(F.min("sale_date").alias("first_sale_date"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(F.col("p.stock_qty"), F.col("t.first_sale_date"))
    .orderBy(F.col("stock_qty").desc())
)
