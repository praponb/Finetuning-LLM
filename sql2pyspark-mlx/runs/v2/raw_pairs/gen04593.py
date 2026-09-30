from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.length("product_name") > 6) & (F.col("launch_date") >= "2025-01-22") & (F.col("stock_qty") <= 327))
    .agg(
        F.sum(F.when(F.col("brand") == "Acme", F.col("stock_qty")).otherwise(0)).alias("acme_stock_qty"),
        F.max("cost").alias("max_cost"),
    )
)
