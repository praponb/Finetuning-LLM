from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.col("shipped_date").alias("event_date"))
    .unionByName(df_products.select(F.col("launch_date").alias("event_date")))
    .orderBy("event_date")
)
