from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.product_id").isin([1, 2, 4, 8]))
    .select(
        F.col("ss.store_id"),
        F.col("ss.sale_date"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        (F.col("ss.revenue") - F.col("ss.units")).alias("revenue_per_unit"),
    )
)
