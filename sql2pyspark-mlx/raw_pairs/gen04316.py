from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .groupBy(
        F.when(F.col("suppliers.rating") >= 4.4, "high").when(F.col("suppliers.rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.max(F.col("store_sales.sale_date")).alias("latest_sale_date"))
    .orderBy(F.col("latest_sale_date").desc(), F.col("rating_bucket").desc())
    .limit(10)
)
