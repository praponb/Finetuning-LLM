from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .groupBy(
        F.when(F.col("suppliers.rating") >= 4.5, "high").when(F.col("suppliers.rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.min(F.col("store_sales.revenue")).alias("min_revenue"))
    .filter(F.col("min_revenue") >= 3700)
    .orderBy("min_revenue", "rating_bucket")
)
