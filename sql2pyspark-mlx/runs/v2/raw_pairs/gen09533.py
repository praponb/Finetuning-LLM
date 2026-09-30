from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("shipped_date") >= "2024-05-01") & (F.col("cost") < 77))
    .select("id", "status", "order_id", "delivered_date")
)
