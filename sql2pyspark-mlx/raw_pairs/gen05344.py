from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter((F.col("shipped_date") >= F.to_date(F.lit("2023-03-12"))) & (F.col("order_id") == 22))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        "status",
        F.date_sub("shipped_date", 3).alias("shipped_date_minus_3d"),
        (F.datediff("delivered_date", "shipped_date") / 7).alias("weeks_shipped_date_to_delivered_date"),
    )
)

result = (
    filtered
    .groupBy("order_id")
    .pivot("status", ["DELIVERED", "LOST"])
    .agg(F.sum("weeks_shipped_date_to_delivered_date"))
)
