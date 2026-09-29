from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "outer")
    .filter(F.col("products.launch_date") >= "2023-03-30")
    .groupBy(
        F.col("suppliers.city"),
        F.when(F.col("suppliers.rating") >= 4.0, "high").when(F.col("suppliers.rating") >= 2.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        (F.max(F.col("store_sales.units")) - F.min(F.col("store_sales.units"))).alias("units_range"),
    )
)
