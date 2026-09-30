from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("order_id") == 30) & F.col("order_id").isin([1, 14, 16]) & (F.col("delivered_date") < "2024-09-01"))
    .select("shipped_date", "delivered_date", "cost")
)
