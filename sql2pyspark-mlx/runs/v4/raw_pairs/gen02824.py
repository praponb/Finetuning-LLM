from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .groupBy(F.col("stores.region"))
    .agg(
        (F.max(F.col("store_sales.units")) - F.min(F.col("store_sales.units"))).alias("units_range"),
    )
    .orderBy("region")
)
