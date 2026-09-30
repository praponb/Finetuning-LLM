from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["Kerry", "Flash"]) & (F.col("delivered_date") < "2024-10-01") & (F.col("cost") < 80))
    .select("id", "order_id")
)
