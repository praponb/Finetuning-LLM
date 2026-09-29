from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(F.col("status").isin(["DELIVERED", "LOST"]))
    .withColumn("days_shipped_date_to_delivered_date", F.datediff("delivered_date", "shipped_date"))
)

agg = (
    base
    .groupBy("carrier")
    .agg(
        F.min("days_shipped_date_to_delivered_date").alias("min_days_shipped_date_to_delivered_date"),
        F.avg("days_shipped_date_to_delivered_date").alias("avg_days_shipped_date_to_delivered_date"),
    )
)

result = agg.orderBy("min_days_shipped_date_to_delivered_date")
