from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(F.min("delivered_date").alias("first_delivered_date"))
    .orderBy(F.col("first_delivered_date").desc())
)
