from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "DELIVERED"]))
    .select("carrier", "delivered_date")
    .unionByName(
        df_shipments
        .filter(F.col("order_id").isin([11, 19, 40]))
        .select("carrier", "delivered_date")
    )
)
