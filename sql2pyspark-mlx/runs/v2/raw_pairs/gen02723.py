from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") >= 87) | (F.col("shipped_date") > "2025-10-01"))
    .groupBy("carrier")
    .agg(F.min("shipped_date").alias("first_shipped_date"))
    .orderBy(F.col("first_shipped_date").desc())
)
