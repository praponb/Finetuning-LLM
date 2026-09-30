from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.month("delivered_date").alias("delivered_month"))
    .agg(F.max("delivered_date").alias("last_delivered_date"))
    .orderBy("last_delivered_date", F.col("delivered_month").desc())
)
