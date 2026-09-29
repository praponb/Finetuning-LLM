from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("status").orderBy("order_date_month_end", "id")
w3 = Window.partitionBy("status")
w_recent = Window.orderBy(F.col("corr_amount_discount").asc_nulls_last())
w5 = Window.orderBy(F.col("amount_per_discount").asc_nulls_last())
w_running = Window.partitionBy("store_id").orderBy(F.col("prev_amount_per_discount").desc())

base = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-05-14").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.when(F.col("channel").isin(["partner", "web"]), "Core").otherwise("Other").alias("channel_group"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

valid = (
    base
    .filter(~F.col("status").isin(["COMPLETED", "PENDING", "RETURNED"]))
    .select(
        "id",
        "store_id",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        "resolved_channel",
        F.last_day("order_date").alias("order_date_month_end"),
        (F.col("amount") / F.nullif("discount", F.lit(0))).alias("amount_per_discount"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.corr("amount", "discount").over(w).alias("corr_amount_discount"),
        F.lag("amount_per_discount", 1).over(w2).alias("prev_amount_per_discount"),
    )
)

matched_keys = (
    metrics
    .select("status")
    .exceptAll(
        df_orders
        .filter(F.col("channel") == "mobile")
        .select("status")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.opened_date"))
)

enriched = (
    joined
    .withColumn("status_max_prev_amount_per_discount", F.max("prev_amount_per_discount").over(w3))
    .withColumn("corr_amount_discount_rank", F.rank().over(w_recent))
    .withColumn("amount_per_discount_rank", F.rank().over(w5))
    .withColumn("prev_amount_per_discount_percent_rank", F.percent_rank().over(w_running))
)

result = (
    enriched
    .select(
        "store_id",
        "shipping_fee",
        F.col("prev_amount_per_discount").alias("PREV_AMOUNT_PER_DISCOUNT"),
    )
    .unpivot(
        ["store_id"],
        ["shipping_fee", "PREV_AMOUNT_PER_DISCOUNT"],
        "metric",
        "val",
    )
    .filter(F.col("val").isNotNull())
)
