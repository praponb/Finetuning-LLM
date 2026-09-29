from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.brand").isin(["Umbrella", "Initech"]))
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("ss.store_id"),
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("p.product_name"),
        F.col("p.supplier_id"),
        F.length(F.col("s.region")).alias("region_length"),
    )
    .orderBy("product_name", "id")
    .limit(10)
)
