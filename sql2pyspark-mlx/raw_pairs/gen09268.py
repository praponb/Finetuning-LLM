from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.delivered_date",
        "src.cost",
        F.col("b.amount"),
        F.col("b.order_date"),
    )
)
