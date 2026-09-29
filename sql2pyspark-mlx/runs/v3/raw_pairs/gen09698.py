from pyspark.sql import functions as F

result = (
    df_shipments
    .select("shipped_date")
    .unionByName(df_products.select(F.col("launch_date").alias("shipped_date")))
)
