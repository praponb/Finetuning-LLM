from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") < "2025-04-11") & (F.col("order_id") == 31) & (F.col("cost") <= 63))
    .select("id", "cost", "shipped_date", "carrier")
)
