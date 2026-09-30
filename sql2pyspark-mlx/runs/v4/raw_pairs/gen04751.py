from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") >= 39)
    .select("shipped_date")
    .intersect(df_products.select(F.col("launch_date").alias("shipped_date")))
)
