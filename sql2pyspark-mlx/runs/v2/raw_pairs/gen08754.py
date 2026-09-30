from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") >= 85)
    .select("id", "order_id", "delivered_date")
    .unionByName(
        df_shipments
        .filter(F.col("carrier") == "Flash")
        .select("id", "order_id", "delivered_date")
    )
    .distinct()
)
