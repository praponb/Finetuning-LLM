from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.array_contains("tags", "premium") & (F.col("launch_date") > "2023-06-01"))
)

summary = (
    base
    .groupBy("brand", "category")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
)

result = (
    summary
    .filter(F.col("stddev_cost") >= 59)
    .select("brand", "stddev_cost")
    .orderBy("stddev_cost")
)
