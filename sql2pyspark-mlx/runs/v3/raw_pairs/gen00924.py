from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("order_id") == 21) & (F.year("delivered_date") == 2023) & (F.col("shipped_date") >= "2025-05-01"))
    .select("delivered_date", "cost")
    .orderBy("delivered_date")
)
