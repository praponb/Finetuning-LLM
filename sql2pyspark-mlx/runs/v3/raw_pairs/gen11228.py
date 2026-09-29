from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("brand") == "Stark") & (F.col("stock_qty") != 176) & F.col("product_name").like("B%"))
    .groupBy("brand")
    .agg(
        (F.max("price") - F.min("price")).alias("price_range"),
        F.round(F.stddev("price"), 2).alias("stddev_price"),
        F.avg("price").alias("avg_price"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") >= 5)
    .drop("total_count")
)
