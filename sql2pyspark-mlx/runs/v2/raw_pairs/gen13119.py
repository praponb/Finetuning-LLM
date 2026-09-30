from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(
        F.round(F.stddev("price"), 2).alias("stddev_price"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_count"),
    )
    .filter(F.col("sports_count") < 4)
    .drop("sports_count")
    .orderBy(F.col("launch_quarter").desc())
    .limit(10)
)
