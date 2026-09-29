from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.count("price").alias("price_count"),
        F.max("launch_date").alias("latest_launch_date"),
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
    )
    .orderBy("brand")
    .limit(5)
)
