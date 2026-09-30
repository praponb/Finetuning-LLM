from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .select(F.col("shipped_date").alias("event_date"))
    .intersect(df_products.select(F.col("launch_date").alias("event_date")))
)
