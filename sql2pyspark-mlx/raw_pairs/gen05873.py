from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("least_shipping_fee_amount").asc_nulls_last(), "id")
w2 = Window.partitionBy("customer_id", "status").orderBy(F.col("amount").desc(), "id")

base = (
    df_orders
    .filter(F.col("shipping_fee").between(11, 12))
    .select(
        "*",
        F.least("shipping_fee", "amount").alias("least_shipping_fee_amount"),
        F.when(F.col("channel") == "web", 1).otherwise(0).alias("is_web"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

deduped = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

latest = (
    deduped
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

cleaned = (
    latest
    .filter((F.col("order_date") > F.lit("2023-09-12").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "discount",
        "shipping_fee",
        "least_shipping_fee_amount",
        "is_web",
        "resolved_channel",
        "row_num",
        (F.col("is_web") * F.col("shipping_fee")).alias("is_web_times_shipping_fee"),
        F.when(F.col("least_shipping_fee_amount") >= 77, "High").when(F.col("least_shipping_fee_amount") >= 26, "Medium").otherwise("Low").alias("least_shipping_fee_amount_tier"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

filtered = (
    cleaned
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "status",
        "discount",
        "shipping_fee",
        "resolved_channel",
        "row_num",
        "least_shipping_fee_amount_tier",
        "channel_filled",
        (F.col("discount") + F.col("is_web")).alias("discount_plus_is_web"),
    )
)

matched_keys = (
    filtered
    .select("id")
    .intersectAll(
        df_orders
        .filter(F.col("discount") < 0.12)
        .select("id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.quantity") <= 6), "left_anti")
)

result = (
    kept
    .groupBy("status")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
    )
    .filter(F.col("row_count") > 1)
)
