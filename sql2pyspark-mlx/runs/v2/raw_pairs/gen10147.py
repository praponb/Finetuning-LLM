from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.launch_date") < "2024-10-01")
    .groupBy(F.col("p.brand"))
    .agg(
        F.count(F.col("ss.units")).alias("units_count"),
        F.min(F.col("ss.units")).alias("min_units"),
    )
)
