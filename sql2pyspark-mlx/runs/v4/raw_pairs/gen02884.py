from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("product_name").like("No%")).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_semi")
    .select(F.col("ss.id"), F.col("ss.product_id"))
    .orderBy(F.col("id").desc())
)
