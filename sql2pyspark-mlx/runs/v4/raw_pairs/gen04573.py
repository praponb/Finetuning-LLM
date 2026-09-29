from pyspark.sql import functions as F

src = df_shipments.filter(F.col("status") != "DELIVERED")

agg = (
    src
    .groupBy("status", F.month("delivered_date").alias("delivered_month"))
    .agg(F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_count"))
)

result = agg
