from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .groupBy(
        F.when(F.year(F.col("store_sales.sale_date")) < 2024, "before_2024").when(F.year(F.col("store_sales.sale_date")) == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
        F.when(F.col("store_sales.revenue") >= 4100, "high").when(F.col("store_sales.revenue") >= 1200, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(F.min(F.col("inventory.last_restock")).alias("min_last_restock"))
)
