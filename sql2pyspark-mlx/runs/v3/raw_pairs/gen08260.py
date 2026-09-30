from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Kerry", "DHL"]) & F.month("delivered_date").isin([3, 4, 5]))
    .select("status", "cost", "order_id")
)
