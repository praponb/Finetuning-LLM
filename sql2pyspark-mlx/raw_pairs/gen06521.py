from pyspark.sql import functions as F

cost_by_order_id = df_shipments.groupBy("order_id").agg(F.min("cost").alias("min_shipment_cost"))

filtered = (
    df_payments
    .filter(F.col("amount").between(580, 820))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.upper("status").alias("status_upper"),
        F.coalesce("method", F.lit("N/A")).alias("resolved_method"),
    )
)

joined = (
    filtered
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.amount") >= 1570)
    .select(
        "e.id",
        "e.order_id",
        "e.status",
        "e.resolved_method",
        F.col("d.status").alias("order_status"),
        F.col("d.channel"),
    )
)

valid = (
    joined
    .filter(F.col("order_id").isNull())
    .select(
        "id",
        "order_id",
        "status",
        "resolved_method",
        "order_status",
        "channel",
        F.length("channel").alias("channel_length"),
        F.initcap(F.trim("status")).alias("status_title"),
        F.coalesce("order_status", F.lit("NONE")).alias("order_status_filled"),
    )
)

cleaned = (
    valid
    .filter(F.col("order_status").isNotNull() & (F.col("channel").eqNullSafe(F.lit("web"))))
    .select("*", F.upper("channel").alias("channel_upper"))
)

enriched = (
    cleaned
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.discount"))
)

result = (
    enriched
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "status",
        "order_status",
        "channel",
        "status_title",
        "order_status_filled",
        "channel_upper",
        "discount",
        F.when(F.col("channel").rlike("^(partner|web)$"), "GROUP_A").otherwise("PRIORITY").alias("channel_zone"),
        F.col("min_shipment_cost"),
    )
    .orderBy(F.col("min_shipment_cost").asc_nulls_first())
)
