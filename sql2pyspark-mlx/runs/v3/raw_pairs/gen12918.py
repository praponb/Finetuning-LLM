from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("ss.units") < 34)
    .select(
        F.col("p.product_name"),
        F.col("p.price"),
        F.col("p.category"),
        F.col("ss.revenue"),
        F.col("ss.store_id"),
    )
    .orderBy(F.col("store_id").desc())
)
