from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .unionByName(df_shipments.select(F.col("delivered_date").alias("launch_date")))
)
