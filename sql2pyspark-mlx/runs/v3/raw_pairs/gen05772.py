from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "right")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .filter(F.col("ss.units").isNotNull())
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("s.sqft"),
        F.col("p.category"),
        F.col("p.brand"),
    )
    .orderBy("brand")
)
