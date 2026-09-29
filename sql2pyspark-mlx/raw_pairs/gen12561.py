from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") <= "2024-10-13") | F.col("carrier").isNull())
    .withColumn("days_delivered_date_to_shipped_date", F.datediff("shipped_date", "delivered_date"))
)
