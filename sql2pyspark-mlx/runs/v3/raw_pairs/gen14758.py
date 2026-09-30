from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "outer")
    .filter(F.month(F.col("products.launch_date")).isin([5, 6, 7]))
    .groupBy(
        F.col("products.supplier_id"),
        F.when(F.col("products.category").isin(["Books", "Home"]), "priority").otherwise("other").alias("category_group"),
    )
    .agg(F.max(F.col("store_sales.units")).alias("max_units"))
)
