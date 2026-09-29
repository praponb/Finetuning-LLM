from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([5, 7, 14, 38]) & F.col("order_id").isNotNull() & (F.col("status") == "DELIVERED"))
    .select("id", "carrier", "shipped_date", "order_id")
)
