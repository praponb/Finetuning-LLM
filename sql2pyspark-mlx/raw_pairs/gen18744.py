from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter(((F.size("tags") >= 1) & (F.col("cost") >= 570)) | F.array_contains("tags", "clearance"))
    .withColumn("stock_qty_per_cost", F.round((F.col("stock_qty") / F.col("cost")), 2))
)

launch_month_summary = (
    filtered_products
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(
        F.sum(F.when(F.col("category") == "Sports", F.col("stock_qty")).otherwise(0)).alias("sports_stock_qty"),
    )
)

result = (
    launch_month_summary
    .filter(F.col("sports_stock_qty") >= 25)
    .select("launch_month", "sports_stock_qty")
    .orderBy("sports_stock_qty", "launch_month")
    .limit(50)
)
