from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(((F.col("delivered_date") >= "2025-03-01") & (F.col("shipped_date") > "2023-06-01")) | F.col("shipped_date").between("2023-03-03", "2024-01-19"))
    .select("order_id", "status", "cost")
)
