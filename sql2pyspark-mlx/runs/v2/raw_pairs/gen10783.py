from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Sports", "Fashion", "Electronics"]))
    .groupBy("category", F.month("launch_date").alias("launch_month"))
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
)
