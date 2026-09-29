from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.filter(F.col("stock_qty") >= 132).alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left_anti")
    .filter(F.col("store_sales.revenue").between(1600, 1800))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.sale_date"),
        F.col("store_sales.store_id"),
    )
)
