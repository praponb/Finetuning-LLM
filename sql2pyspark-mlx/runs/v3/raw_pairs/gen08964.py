from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.year("delivered_date") == 2024) & (F.col("order_id") == 5) & (F.col("delivered_date") > "2025-03-01"))
    .groupBy("order_id")
    .agg(F.min("shipped_date").alias("first_shipped_date"))
    .orderBy(F.col("order_id").desc())
)
