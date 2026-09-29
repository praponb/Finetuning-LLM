from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)
