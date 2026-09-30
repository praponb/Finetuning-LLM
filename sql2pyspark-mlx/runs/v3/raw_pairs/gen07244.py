from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("ss.units"),
        F.col("ss.revenue"),
        F.col("ss.product_id"),
    )
)
