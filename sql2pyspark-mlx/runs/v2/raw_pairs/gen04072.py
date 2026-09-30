from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("stock_qty") > 346)
    .withColumn("sorted_tags", F.sort_array("tags"))
)

launch_month_summary = (
    base
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Grocery", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grocery"),
        F.count("*").alias("cnt"),
    )
)

result = launch_month_summary.filter(F.col("pct_grocery") > 55)
