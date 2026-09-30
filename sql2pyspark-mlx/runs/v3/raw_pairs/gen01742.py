from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        (F.max("units") - F.min("units")).alias("units_range"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
)

result = (
    df_products.alias("products")
    .join(store_sales_by_product.alias("t"), F.col("products.id") == F.col("t.product_id"), "left")
    .select(F.col("products.supplier_id"), F.col("t.units_range"), F.col("t.avg_units"))
)
