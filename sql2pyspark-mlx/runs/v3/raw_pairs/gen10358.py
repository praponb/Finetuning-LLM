from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "right")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .groupBy(F.col("stores.region"))
    .agg(
        F.min(F.col("store_sales.sale_date")).alias("first_sale_date"),
        F.count(F.col("store_sales.revenue")).alias("revenue_count"),
    )
    .orderBy("first_sale_date", "region")
    .limit(1)
)
