from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.count("*").alias("product_count"),
        F.max("stock_qty").alias("max_stock_qty"),
        F.sum("cost").alias("total_cost"),
    )
)
