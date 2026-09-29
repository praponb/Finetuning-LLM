from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") == "RETURNED") & (F.col("shipped_date") >= "2025-06-12"))
    .select("id", "order_id")
    .orderBy("order_id")
)
