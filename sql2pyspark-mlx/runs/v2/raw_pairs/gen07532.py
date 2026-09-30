from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Backp%"))
    .groupBy("brand", F.year("launch_date").alias("launch_year"))
    .agg(F.round(F.stddev("price"), 2).alias("stddev_price"))
    .filter(F.col("stddev_price") < 36)
)
