from pyspark.sql import functions as F

result = (
    df_shipments
    .withColumn("months_shipped_date_to_delivered_date", F.round((F.datediff("delivered_date", "shipped_date") / 30), 1))
    .withColumn("days_delivered_date_to_shipped_date", F.datediff("shipped_date", "delivered_date"))
    .orderBy("order_id", "id")
)
