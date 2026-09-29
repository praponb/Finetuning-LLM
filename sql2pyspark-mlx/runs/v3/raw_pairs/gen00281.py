from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .filter(F.col("store_sales.units") < 15)
    .groupBy(F.col("suppliers.city"))
    .agg(F.min(F.col("store_sales.revenue")).alias("min_revenue"))
    .filter(F.col("min_revenue") >= 2900)
    .orderBy(F.col("city").desc())
    .limit(1)
)
