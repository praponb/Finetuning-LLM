from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "outer")
    .filter(F.col("ss.revenue") > 1600)
    .select(
        F.col("ss.product_id"),
        F.col("ss.revenue"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
    )
)
