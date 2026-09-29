from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("s.sqft") == 6612)
    .select(
        F.col("ss.units"),
        F.col("ss.revenue"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("p.product_name"),
        F.col("p.category"),
    )
    .orderBy(F.col("city").desc(), "product_name")
)
