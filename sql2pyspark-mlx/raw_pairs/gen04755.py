from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.month("delivered_date").alias("delivered_month"))
    .agg(F.min("shipped_date").alias("first_shipped_date"))
)
