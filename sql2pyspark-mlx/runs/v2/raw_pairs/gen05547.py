from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.units"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.substring(F.col("s.region"), 1, 4).alias("region_prefix"),
    )
)
