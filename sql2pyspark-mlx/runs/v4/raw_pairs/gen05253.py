from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") > "2024-08-01")
    .groupBy("status", "carrier")
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)
