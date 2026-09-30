from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("carrier").orderBy("cost", "id")

cleaned = (
    df_shipments
    .filter(F.col("delivered_date") >= F.to_date(F.lit("2023-06-01")))
    .withColumn("delivered_date_minus_2d", F.date_sub("delivered_date", 2))
)

enriched = (
    cleaned
    .select(
        "*",
        F.lag("cost").over(w_recent).alias("prev_cost"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)

joined = (
    enriched
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.cost",
        "src.status",
        "src.delivered_date_minus_2d",
        "src.prev_cost",
        "src.event_seq",
        F.col("d.amount"),
        F.col("d.shipping_fee"),
    )
)

cleaned_shipments = (
    joined
    .filter(F.col("prev_cost").between(37, 45))
    .select(
        "*",
        F.datediff(F.current_date(), "delivered_date_minus_2d").alias("days_since_delivered_date_minus_2d"),
    )
)

with_dims = (
    cleaned_shipments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select("s.*", F.col("o.status").alias("order_status"))
)

result = (
    with_dims
    .select(
        "id",
        "cost",
        "delivered_date_minus_2d",
        "event_seq",
        "amount",
        "shipping_fee",
        "order_status",
        F.when(F.col("order_status").rlike("^(PENDING|SHIPPED)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("order_status_zone"),
    )
    .orderBy("order_status_zone")
)
