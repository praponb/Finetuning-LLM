from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(F.col("cost") <= 37)

agg = (
    recent_shipments
    .groupBy("status")
    .agg(F.min("cost").alias("min_cost"))
)

result = agg
