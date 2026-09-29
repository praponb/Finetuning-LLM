from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("ss.product_id"),
        F.col("p.launch_date"),
        F.col("p.cost"),
        F.col("s.city"),
        F.col("ss.units").cast("string").alias("units_str"),
    )
    .orderBy(F.col("launch_date").desc())
)
