from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") >= 560)
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.round(((100 * F.sum(F.when(F.col("category") == "Fashion", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fashion"),
    )
    .filter(F.col("pct_fashion") >= 25)
    .orderBy("latest_launch_date")
)
