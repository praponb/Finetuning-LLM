from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(F.col("ss.units") < 27)
    .select(
        F.col("ss.store_id"),
        F.col("s.sqft"),
        F.col("s.city"),
        F.col("p.launch_date"),
        F.col("p.product_name"),
        F.coalesce(F.col("s.store_name"), F.lit("unassigned")).alias("store_name_filled"),
    )
)
