from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "IN_TRANSIT")
    .select("id", "order_id")
    .unionByName(
        df_shipments
        .filter(F.col("status").isin(["LOST", "DELIVERED"]))
        .select("id", "order_id")
    )
    .orderBy("order_id")
)
