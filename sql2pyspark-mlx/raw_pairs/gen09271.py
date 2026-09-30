from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("store_id").isin([1, 3, 4, 6])).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_semi")
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("p.brand"),
        F.col("p.product_name"),
    )
    .orderBy(F.col("id").desc())
)
