from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter((F.col("ss.sale_date") <= "2024-01-13") & (F.col("ss.units") >= 24))
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.revenue"),
        F.col("p.category"),
        F.col("p.price"),
        F.col("s.sqft"),
        F.last_day(F.col("s.opened_date")).alias("opened_date_month_end"),
    )
)
