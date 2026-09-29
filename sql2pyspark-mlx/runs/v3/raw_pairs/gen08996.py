from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .groupBy("product_id")
    .agg((F.max("units") - F.min("units")).alias("units_range"))
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.units_range") > 75)
    .select(F.col("p.supplier_id"), F.col("t.units_range"))
)
