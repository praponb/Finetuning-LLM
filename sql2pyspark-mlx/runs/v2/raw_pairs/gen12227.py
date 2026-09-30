from pyspark.sql import functions as F

result = (
    df_shipments
    .withColumn("days_delivered_date_to_shipped_date", F.datediff("shipped_date", "delivered_date"))
    .withColumn("carrier_lower", F.lower("carrier"))
)
