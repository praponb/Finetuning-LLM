from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") != 348) | F.col("stock_qty").between(292, 396))
    .agg(
        F.max("price").alias("max_price"),
        F.max("launch_date").alias("latest_launch_date"),
        F.sum("stock_qty").alias("total_stock_qty"),
    )
)
