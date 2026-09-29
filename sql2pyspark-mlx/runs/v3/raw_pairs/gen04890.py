from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(60, 72))
    .withColumn("cost_filled", F.coalesce("cost", F.lit(0)))
    .withColumn("days_delivered_date_to_shipped_date", F.datediff("shipped_date", "delivered_date"))
)
