from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.count("stock_qty").alias("stock_qty_count"),
        F.min("stock_qty").alias("min_stock_qty"),
        F.min("cost").alias("min_cost"),
    )
)
