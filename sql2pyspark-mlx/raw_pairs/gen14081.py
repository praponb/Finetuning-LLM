from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter((F.col("shipped_date") >= F.to_date(F.lit("2023-08-20"))) & F.month("delivered_date").isin([5, 9, 10]))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.date_format("delivered_date", "yyyy-MM-dd").alias("delivered_date_label"),
        F.coalesce("carrier", F.lit("NONE")).alias("resolved_carrier"),
    )
)

result = (
    filtered
    .select("status", "order_id")
    .subtract(
        df_shipments
        .filter(F.col("order_id") == 14)
        .select("status", "order_id")
    )
)
