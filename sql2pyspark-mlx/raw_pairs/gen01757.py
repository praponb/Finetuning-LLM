from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("p.brand") == "Stark")
    .select(
        F.col("ss.product_id"),
        F.col("ss.store_id"),
        F.col("s.region"),
        F.col("p.cost"),
    )
)
