from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status", F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"))
    .agg(F.max("shipped_date").alias("last_shipped_date"))
)
