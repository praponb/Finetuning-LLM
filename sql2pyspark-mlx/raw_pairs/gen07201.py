from pyspark.sql import functions as F

result = (
    df_stores
    .select(F.col("opened_date").alias("event_date"))
    .subtract(df_shipments.select(F.col("delivered_date").alias("event_date")))
)
