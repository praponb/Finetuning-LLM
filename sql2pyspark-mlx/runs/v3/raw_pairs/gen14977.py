from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "FedEx")
    .select("id", "carrier", "order_id")
    .unionByName(
        df_shipments
        .filter(F.col("shipped_date") >= "2025-07-01")
        .select("id", "carrier", "order_id")
    )
    .distinct()
    .orderBy("order_id")
)
