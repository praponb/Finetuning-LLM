from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "left")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .filter(F.length(F.col("stores.store_name")) > 14)
    .groupBy(F.col("products.brand"), F.col("products.supplier_id"))
    .agg(F.sum(F.col("store_sales.units")).alias("total_units"))
    .filter(F.col("total_units") >= 220)
    .orderBy(F.col("total_units").desc(), F.col("brand").desc())
)
