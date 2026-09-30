from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("products.id") == F.col("store_sales.product_id"), "left")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .groupBy(
        F.date_trunc("MONTH", F.col("stores.opened_date")).alias("opened_month"),
        F.when(F.col("stores.sqft") >= 17820, "high").when(F.col("stores.sqft") >= 6122, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.sum(F.col("store_sales.units")).alias("total_units"),
        F.min(F.col("store_sales.sale_date")).alias("first_sale_date"),
    )
    .orderBy(F.col("sqft_bucket").desc(), F.col("opened_month").desc())
    .limit(5)
)
