from pyspark.sql import functions as F

result = (
    df_shipments
    .select("carrier", "order_id")
    .intersect(
        df_shipments
        .filter(F.col("status").isin(["RETURNED", "LOST", "DELIVERED"]))
        .select("carrier", "order_id")
    )
)
