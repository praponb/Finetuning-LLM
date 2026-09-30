from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("revenue") <= 1700).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_semi")
    .select(F.col("p.id"), F.col("p.cost"), F.col("p.brand"), F.col("p.supplier_id"))
    .orderBy(F.col("id").desc())
)
