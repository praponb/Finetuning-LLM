from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("supplier_id").isin([22, 34])).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("ss.revenue") >= 2200)
    .select(F.col("ss.id"), F.col("ss.product_id"), F.col("ss.revenue"))
)
