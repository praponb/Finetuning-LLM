from pyspark.sql import functions as F

valid = (
    df_shipments
    .filter((F.col("status") != "RETURNED") & (F.col("delivered_date") >= F.lit("2023-07-03").cast("date")))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.trim("carrier").alias("clean_carrier"),
    )
)

result = (
    valid
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .filter(F.month(F.col("d.order_date")).isin([1, 4, 5]))
    .select(
        "e.id",
        "e.order_id",
        F.col("d.channel"),
        F.col("d.discount"),
        F.col("d.shipping_fee"),
    )
)
