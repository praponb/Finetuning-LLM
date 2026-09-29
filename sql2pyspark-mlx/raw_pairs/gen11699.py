from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-04-30").cast("date"))
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        F.add_months("order_date", 1).alias("order_date_plus_months"),
    )
)

result = (
    filtered
    .select("channel", "id")
    .exceptAll(
        df_orders
        .filter(F.col("amount").between(1090, 1430))
        .select("channel", "id")
    )
)
