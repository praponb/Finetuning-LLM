from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("p.brand"))
    .agg(
        F.count(F.col("ss.revenue")).alias("revenue_count"),
        F.min(F.col("ss.sale_date")).alias("first_sale_date"),
    )
    .orderBy("brand")
    .limit(3)
)
