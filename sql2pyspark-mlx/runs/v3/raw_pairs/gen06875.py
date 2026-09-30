from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") < "2024-04-02")
    .select("order_id", "cost", "carrier", F.lower("carrier").alias("carrier_lower"))
)
