from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "left")
    .filter(F.col("store_sales.units").between(22, 36))
    .groupBy(
        F.when(F.col("stores.sqft") >= 17896, "high").when(F.col("stores.sqft") >= 4160, "mid").otherwise("low").alias("sqft_bucket"),
        F.when(F.year(F.col("stores.opened_date")) < 2024, "before_2024").when(F.year(F.col("stores.opened_date")) == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(F.count("*").alias("total_count"))
)
