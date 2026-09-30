from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "DELIVERED")
    .groupBy("carrier")
    .agg(F.min("shipped_date").alias("first_shipped_date"))
)
