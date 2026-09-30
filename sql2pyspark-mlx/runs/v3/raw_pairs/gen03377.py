from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "IN_TRANSIT")
    .select("carrier", "delivered_date")
    .intersect(
        df_shipments
        .filter(F.col("status").isin(["DELIVERED", "RETURNED"]))
        .select("carrier", "delivered_date")
    )
)
