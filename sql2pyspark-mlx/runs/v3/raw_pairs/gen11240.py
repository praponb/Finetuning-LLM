from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Stark", "Umbrella", "Initech"]))
    .agg(
        F.round(F.stddev("price"), 2).alias("stddev_price"),
        F.count("stock_qty").alias("stock_qty_count"),
    )
)
