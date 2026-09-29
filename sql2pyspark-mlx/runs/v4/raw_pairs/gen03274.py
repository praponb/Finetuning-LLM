from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.cost") < 590)
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("ss.product_id"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("p.launch_date"),
    )
    .orderBy("launch_date")
)
