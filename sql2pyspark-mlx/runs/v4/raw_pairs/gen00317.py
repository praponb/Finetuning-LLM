from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(
        F.sum(F.when(F.col("category") == "Sports", 1).otherwise(0)).alias("sports_count"),
    )
    .filter(F.col("sports_count") >= 73)
    .select("launch_month", "sports_count")
    .orderBy(F.col("launch_month").desc())
    .limit(3)
)
