from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "outer")
    .groupBy(
        F.when(F.col("suppliers.rating") >= 4.2, "high").when(F.col("suppliers.rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.max(F.col("store_sales.revenue")).alias("max_revenue"))
)
