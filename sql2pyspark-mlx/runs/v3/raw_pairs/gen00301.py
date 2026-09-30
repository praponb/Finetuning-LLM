from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & F.col("shipped_date").between("2024-03-01", "2024-06-01"))
    .groupBy("carrier")
    .agg(F.min("delivered_date").alias("min_delivered_date"))
)
