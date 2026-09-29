from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "left")
    .filter(F.lower(F.col("stores.store_name")).like("%ive%"))
    .groupBy(F.col("stores.region"))
    .agg(
        F.round(F.avg(F.col("store_sales.units")), 2).alias("avg_units"),
        F.avg(F.col("store_sales.revenue")).alias("avg_revenue"),
    )
    .orderBy(F.col("avg_revenue").desc(), F.col("region").desc())
)
