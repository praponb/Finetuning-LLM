from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("product_name").like("%Head%")).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("ss.revenue") < 3500)
    .select(F.col("ss.id"), F.col("ss.revenue"))
    .orderBy(F.col("revenue").desc())
)
