from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .agg(F.min("cost").alias("min_cost"))
)
