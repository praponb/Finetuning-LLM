from pyspark.sql import functions as F

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
    )
)

matched_keys = (
    cleaned
    .select("order_id")
    .intersectAll(
        df_order_items
        .filter(F.col("quantity") <= 4)
        .select("order_id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.amount"))
)

with_dims = (
    joined
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.amount").alias("order_amount"))
)

with_dims_order_items = (
    with_dims
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.customer_id").isNull())
    .select("src.id", "src.order_id", "src.quantity", "src.amount", F.col("b.channel"))
)

result = (
    with_dims_order_items
    .groupBy("order_id")
    .agg(
        F.count_if(F.col("channel") == "mobile").alias("mobile_events"),
        F.round(F.sum("quantity") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("quantity_per_mobile"),
        F.round(F.sum("quantity") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("quantity_per_web"),
        F.avg("amount").alias("avg_amount"),
    )
)
