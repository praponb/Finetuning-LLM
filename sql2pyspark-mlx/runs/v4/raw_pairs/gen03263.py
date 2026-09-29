from pyspark.sql import functions as F

src = (
    df_products
    .filter(~((F.col("supplier_id") == 21) & (F.col("launch_date") <= "2025-09-01")))
    .withColumn("cost_whole", F.ceil("cost"))
)

launch_month_summary = (
    src
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(F.count("cost_whole").alias("cost_whole_count"))
)

result = (
    launch_month_summary
    .select("launch_month", "cost_whole_count")
    .orderBy("cost_whole_count")
)
