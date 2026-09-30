from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(((F.col("delivered_date") <= "2025-10-29") & (F.col("shipped_date") < "2024-08-01")) | F.col("delivered_date").between("2023-03-28", "2023-12-01"))
    .withColumn("shipped_date_minus_12d", F.date_sub("shipped_date", 12))
    .orderBy(F.col("order_id").desc(), "id")
    .limit(3)
)
