from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .pivot("category", ["Home", "Grocery", "Fashion", "Books"])
    .agg(F.avg("price"))
)
