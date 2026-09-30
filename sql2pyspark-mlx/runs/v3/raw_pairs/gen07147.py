from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "right")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .groupBy(
        F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"),
        F.when(F.col("s.sqft") >= 16315, "high").when(F.col("s.sqft") >= 8259, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.countDistinct(F.col("ss.units")).alias("unique_stores"),
        F.max(F.col("ss.revenue")).alias("max_revenue"),
    )
    .orderBy(F.col("max_revenue").desc())
)
