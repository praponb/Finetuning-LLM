from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-04-21", "2025-06-01"))
    .select(
        "id",
        "delivered_date",
        "cost",
        F.concat_ws(",", "carrier", "status").alias("carrier_status_key"),
    )
    .orderBy("cost")
)
