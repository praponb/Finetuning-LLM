from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w_part = Window.partitionBy("order_id").orderBy("discount", "id")
w3 = Window.partitionBy(F.month("shipped_date"), "order_id").orderBy("discount", "id")
w4 = Window.partitionBy("order_id").orderBy("discount", "id").rowsBetween(-2, -1)

cleaned = (
    df_shipments
    .filter(F.col("shipped_date") >= F.lit("2023-06-28").cast("date"))
    .withColumn("delivered_date_label", F.date_format("delivered_date", "MMM yyyy"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("DIRECT")))
)

enriched = (
    cleaned
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.amount") <= 820)
    .select(
        "e.id",
        "e.order_id",
        "e.carrier",
        "e.shipped_date",
        "e.delivered_date",
        F.col("b.channel"),
        F.col("b.discount"),
        F.col("b.order_date"),
    )
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    ranked
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "channel",
        "discount",
        F.ntile(3).over(w_part).alias("discount_bucket"),
        F.lag("discount", 1, 0).over(w3).alias("prev_discount"),
        F.avg("discount").over(w4).alias("discount_avg_2_prior"),
    )
)

matched_keys = (
    metrics
    .select("order_id", "carrier")
    .exceptAll(
        df_shipments
        .filter(F.col("cost") > 61)
        .select("order_id", "carrier")
    )
)

verified = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select(
        "s.order_id",
        "s.shipped_date",
        "s.discount",
        "s.discount_bucket",
        "s.discount_avg_2_prior",
        F.col("p.shipping_fee"),
    )
)

with_dims_shipments = (
    with_dims
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select(
        "s.*",
        F.col("p.shipping_fee").alias("order_shipping_fee"),
        F.col("p.status"),
    )
)

result = (
    with_dims_shipments
    .groupBy("order_id")
    .pivot("status", ["PENDING", "SHIPPED"])
    .agg(F.sum("shipping_fee").alias("amount"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "order_id",
        F.col("PENDING_amount").alias("pending_amount"),
        F.col("PENDING_item_count").alias("pending_item_count"),
        F.col("SHIPPED_amount").alias("shipped_amount"),
        F.col("SHIPPED_item_count").alias("shipped_item_count"),
    )
)
