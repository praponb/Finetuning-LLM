from pyspark.sql import functions as F

kept_keys = (
    df_shipments
    .select("id", "status")
    .intersectAll(
        df_shipments
        .filter(F.col("order_id").isNull())
        .select("id", "status")
    )
)

kept_rows = (
    df_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select(
        "e.order_id",
        "e.shipped_date",
        "e.delivered_date",
        F.col("p.status").alias("order_status"),
        F.col("p.channel"),
    )
)

base = (
    joined
    .filter((F.col("shipped_date") > F.lit("2023-02-12").cast("date")) & F.col("channel").isNotNull())
    .select(
        "*",
        (F.datediff("shipped_date", "delivered_date") / 7).alias("weeks_delivered_date_to_shipped_date"),
    )
)

cleaned = (
    base
    .filter(F.col("order_status").isNotNull())
    .withColumn("order_status_channel_key", F.concat_ws("/", "order_status", "channel"))
)

matched_keys = (
    cleaned
    .select("order_id")
    .intersectAll(
        df_shipments
        .filter(F.col("delivered_date") < "2025-09-01")
        .select("order_id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("order_id")
    .pivot("channel", ["partner", "web"])
    .agg(
        F.sum("weeks_delivered_date_to_shipped_date").alias("amount"),
        F.count(F.lit(1)).alias("item_count"),
    )
)
