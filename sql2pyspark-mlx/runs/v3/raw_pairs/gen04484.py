from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.col("delivered_date").alias("event_date"))
    .unionByName(df_store_sales.select(F.col("sale_date").alias("event_date")))
)
