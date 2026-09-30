from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
    .filter(F.col("cost_range") < 55)
)
