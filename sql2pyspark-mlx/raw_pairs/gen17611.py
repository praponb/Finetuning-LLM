from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~(F.col("order_id").isin([3, 17, 19, 32]) & (F.col("delivered_date") <= "2023-10-01") & F.col("shipped_date").between("2023-05-01", "2025-01-17")))
    .select("status", F.trim("status").alias("clean_status"))
    .orderBy("clean_status")
)
