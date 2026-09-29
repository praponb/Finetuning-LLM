from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
    .filter(F.col("avg_cost") < 29)
    .select("carrier", "avg_cost")
)
