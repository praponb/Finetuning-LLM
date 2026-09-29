from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(F.max("shipped_date").alias("last_shipped_date"))
    .orderBy(F.col("last_shipped_date").desc())
)
