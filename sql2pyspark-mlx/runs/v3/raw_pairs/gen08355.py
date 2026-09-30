from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") == "IN_TRANSIT") & (F.col("shipped_date") > "2025-03-22"))
    .select("order_id", "status")
)
