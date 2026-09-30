from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .filter((F.col("p.price") >= 880) & (F.col("ss.product_id") == 8))
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("p.product_name"),
        F.col("p.brand"),
        F.col("p.category"),
    )
    .orderBy(F.col("revenue").desc())
)
