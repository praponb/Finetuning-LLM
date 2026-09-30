from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.month("sale_date").isin([6, 7, 11])).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_semi")
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("p.cost"))
)
