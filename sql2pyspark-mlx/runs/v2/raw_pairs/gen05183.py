from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["LOST", "IN_TRANSIT", "DELIVERED"]))
    .select("id", "delivered_date", "order_id")
    .unionByName(
        df_shipments
        .filter(F.year("shipped_date") == 2023)
        .select("id", "delivered_date", "order_id")
    )
)
