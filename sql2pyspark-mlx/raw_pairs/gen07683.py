from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter(F.col("delivered_date") >= F.lit("2023-06-15").cast("date"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "status",
        F.when(F.col("status").isin(["LOST", "RETURNED"]), "Group A").otherwise("Other").alias("status_group"),
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
    )
)

kept_keys = (
    filtered
    .select("status")
    .intersectAll(
        df_shipments
        .filter(F.col("order_id").isNotNull())
        .select("status")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select(
        "e.order_id",
        "e.carrier",
        "e.shipped_date",
        "e.status",
        "e.status_group",
        F.col("p.discount"),
    )
)

enriched = (
    joined
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .filter(F.col("d.channel").isNotNull())
    .select(
        "a.order_id",
        "a.carrier",
        "a.status",
        "a.status_group",
        F.col("d.discount").alias("order_discount"),
    )
)

joined_shipments = (
    enriched
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select(
        "e.order_id",
        "e.carrier",
        "e.status",
        "e.order_discount",
        F.col("p.discount"),
        F.col("p.status").alias("order_status"),
    )
)

result = (
    joined_shipments
    .select("carrier", "order_discount", "discount", "order_status")
    .orderBy(F.col("discount").desc_nulls_last())
)
