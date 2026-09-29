from pyspark.sql import functions as F

result = (
    df_shipments
    .withColumn("status_length", F.length("status"))
    .withColumn("delivered_date_period", F.date_trunc("YEAR", "delivered_date"))
)
