from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount_filled")
w2 = Window.partitionBy("order_id").orderBy(F.col("amount").desc())
w3 = Window.partitionBy("channel").orderBy("amount", "id")
w4 = Window.partitionBy("channel", "order_id").orderBy(F.col("amount").desc_nulls_last())
w5 = Window.partitionBy("order_id", "channel").orderBy("rn", "id")

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-05-19 07:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)

with_dims = (
    base
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id").eqNullSafe(F.col("o.id")), "inner")
    .filter(~F.col("o.channel").isin(["web", "mobile", "store"]))
    .select(
        "a.id",
        "a.order_id",
        "a.method",
        "a.status",
        "a.amount_filled",
        F.col("o.discount"),
    )
)

latest = (
    with_dims
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

joined = (
    latest
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.method",
        "s.discount",
        F.col("p.shipping_fee"),
        F.col("p.channel"),
        F.col("p.amount"),
    )
)

ranked = (
    joined
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
)

scored = (
    ranked
    .select(
        "id",
        "order_id",
        "channel",
        "amount",
        "rn",
        F.ntile(4).over(w3).alias("amount_bucket"),
        F.rank().over(w4).alias("amount_rank"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.lag("amount").over(w5).alias("prev_amount"),
        F.lag("amount_bucket").over(w5).alias("prev_amount_bucket"),
        F.lead("amount_bucket").over(w5).alias("next_amount_bucket"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "channel",
        "amount",
        "rn",
        "amount_bucket",
        "amount_rank",
        "prev_amount",
        "prev_amount_bucket",
    )
    .orderBy(F.col("amount").desc_nulls_last())
)
