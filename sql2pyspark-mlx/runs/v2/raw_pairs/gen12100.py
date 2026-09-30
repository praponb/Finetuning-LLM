from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "right")
    .filter(F.col("ss.sale_date") <= "2024-11-29")
    .groupBy(F.month(F.col("s.opened_date")).alias("opened_month"))
    .agg(
        F.max(F.col("ss.sale_date")).alias("latest_sale_date"),
        F.count(F.col("ss.revenue")).alias("revenue_count"),
    )
    .orderBy(F.col("opened_month").desc())
)
