from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") < "2024-12-23")
    .groupBy("carrier", F.date_trunc("MONTH", "shipped_date").alias("shipped_month"))
    .agg(F.min("delivered_date").alias("first_delivered_date"))
    .orderBy("carrier")
)
