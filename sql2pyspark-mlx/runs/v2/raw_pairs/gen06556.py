from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "outer")
    .filter(F.col("s.region").isNull() & (F.col("ss.product_id") == 7))
    .select(
        F.col("ss.revenue"),
        F.col("ss.units"),
        F.col("p.launch_date"),
        F.col("p.brand"),
        F.col("s.store_name"),
        F.col("s.sqft"),
    )
    .orderBy("store_name")
)
