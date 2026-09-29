from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .groupBy(
        F.when(F.col("stores.sqft") >= 14741, "high").when(F.col("stores.sqft") >= 3343, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.min(F.col("store_sales.sale_date")).alias("first_sale_date"),
        F.sum(F.col("store_sales.units")).alias("total_units"),
    )
    .orderBy("first_sale_date")
)
