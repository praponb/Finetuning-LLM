from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.store_id"),
        F.col("p.launch_date"),
        F.col("p.price"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.when(F.col("ss.units") >= 44, "High").when(F.col("ss.units") >= 19, "Medium").otherwise("Low").alias("units_band"),
    )
    .orderBy(F.col("units_band").desc())
)
