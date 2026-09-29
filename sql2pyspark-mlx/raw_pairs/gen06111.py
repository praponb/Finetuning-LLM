from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") < 38)
    .groupBy(F.quarter("shipped_date").alias("shipped_quarter"), "status")
    .agg(F.min("delivered_date").alias("min_delivered_date"))
)
