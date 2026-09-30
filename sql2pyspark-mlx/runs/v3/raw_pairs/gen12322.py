from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(F.round(F.stddev("price"), 2).alias("stddev_price"))
)
