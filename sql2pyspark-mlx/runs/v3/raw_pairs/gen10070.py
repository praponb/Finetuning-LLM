from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2024-04-01", "2024-05-18") & ((F.col("status") != "IN_TRANSIT") | (F.col("shipped_date") <= "2023-06-01")))
    .select(
        "id",
        "order_id",
        "carrier",
        "status",
        F.concat_ws(",", "carrier", "status").alias("carrier_status_key"),
    )
    .orderBy(F.col("status").desc(), F.col("id").desc())
    .limit(20)
)
