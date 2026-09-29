from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.count("*").alias("product_count"),
        F.round(F.stddev("price"), 2).alias("stddev_price"),
    )
)
