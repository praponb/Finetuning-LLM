from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("price") <= 430)
    .withColumn("is_home", F.when(F.col("category") == "Home", 1).otherwise(0))
)

agg = (
    base
    .groupBy("category", F.month("launch_date").alias("launch_month"))
    .agg(F.round(F.stddev("is_home"), 2).alias("stddev_is_home"))
)

result = (
    agg
    .filter(F.col("stddev_is_home") < 79)
    .select("category", "launch_month", "stddev_is_home")
)
