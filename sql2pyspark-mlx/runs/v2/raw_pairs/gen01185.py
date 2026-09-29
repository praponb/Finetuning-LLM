from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2025-01-01", "2025-02-01"))
    .select(
        "shipped_date",
        "order_id",
        "cost",
        F.concat_ws("-", "carrier", "status").alias("carrier_status_key"),
        F.date_trunc("week", "shipped_date").alias("shipped_date_period"),
    )
    .orderBy("cost")
)
