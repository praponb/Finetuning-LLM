from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .subtract(df_shipments.select(F.col("shipped_date").alias("launch_date")))
)
