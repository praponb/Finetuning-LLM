from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("s.sqft") != 10096)
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.revenue"),
        F.col("ss.units"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
    )
)
