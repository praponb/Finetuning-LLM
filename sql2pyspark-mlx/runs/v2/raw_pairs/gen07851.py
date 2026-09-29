from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.category"))
    .agg(
        F.max(F.col("ss.sale_date")).alias("latest_sale_date"),
        F.countDistinct(F.col("ss.revenue")).alias("unique_stores"),
    )
)
