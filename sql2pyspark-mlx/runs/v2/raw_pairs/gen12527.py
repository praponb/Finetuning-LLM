from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.sale_date"),
        F.col("ss.revenue"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("s.store_name"),
    )
)
