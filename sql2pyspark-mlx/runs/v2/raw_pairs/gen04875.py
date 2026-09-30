from pyspark.sql import functions as F

valid_shipments = df_shipments.filter(F.col("order_id").isin([12, 22]))

agg = (
    valid_shipments
    .groupBy("carrier")
    .agg(F.max("shipped_date").alias("latest_shipped_date"))
)

result = (
    agg
    .select("carrier", "latest_shipped_date")
    .orderBy("carrier")
)
