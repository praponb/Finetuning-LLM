from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("shipped_date") > "2025-09-01") & (F.col("cost") >= 31))
    .agg(F.min("shipped_date").alias("min_shipped_date"))
)
