from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter((F.col("shipped_date") >= F.to_date(F.lit("2023-10-20"))) & F.col("order_id").isNotNull() & (F.col("shipped_date") < "2023-08-13"))
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.concat("carrier", F.lit("/"), "status").alias("carrier_status"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.amount"))
)
