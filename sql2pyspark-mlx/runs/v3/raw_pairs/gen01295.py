from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .select(
        F.col("ss.product_id"),
        F.col("ss.store_id"),
        F.col("ss.revenue"),
        F.col("s.opened_date"),
        F.last_day(F.col("ss.sale_date")).alias("sale_date_month_end"),
    )
)
