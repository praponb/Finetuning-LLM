from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") >= 4)
)
