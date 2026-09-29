from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter(F.month("launch_date").isin([1, 5, 11]))
    .withColumn("product_name_title", F.initcap(F.trim("product_name")))
)

launch_quarter_summary = (
    recent_products
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(
        F.sum(F.when(F.col("category") == "Sports", 1).otherwise(0)).alias("sports_count"),
    )
)

result = (
    launch_quarter_summary
    .orderBy(F.col("sports_count").desc(), "launch_quarter")
    .limit(20)
)
