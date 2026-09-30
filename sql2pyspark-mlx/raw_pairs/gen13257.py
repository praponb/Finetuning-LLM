from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("discount").asc_nulls_first())
w_ordered = Window.partitionBy("order_id")
w4 = Window.orderBy(F.col("discount").desc())
w_recent = Window.partitionBy("status")

latest = (
    df_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "carrier", "cost", "status")
)

with_dims = (
    latest
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "inner")
    .filter(F.col("b.shipping_fee") < 17)
    .select("a.id", "a.order_id", "a.carrier", "a.cost", F.col("b.discount"))
)

kept_keys = (
    with_dims
    .select("carrier")
    .subtract(
        df_shipments
        .filter(F.col("order_id").isin([1, 18]))
        .select("carrier")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "order_id",
        "cost",
        "discount",
        F.percent_rank().over(w2).alias("discount_percent_rank"),
        F.count("discount").over(w_ordered).alias("order_id_count_discount"),
        F.rank().over(w4).alias("discount_rank"),
    )
)

enriched = (
    scored
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.amount"), F.col("p.status"), F.col("p.channel"))
)

base = (
    enriched
    .filter(F.col("discount") <= 0.33)
    .select(
        "order_id",
        "discount",
        "discount_percent_rank",
        "discount_rank",
        "amount",
        "status",
        "channel",
        F.round((F.col("cost") * 1.15), 2).alias("cost_scaled"),
        F.coalesce("status", F.lit("OTHER")).alias("resolved_status"),
    )
)

scored_shipments = (
    base
    .select(
        "order_id",
        "discount",
        "discount_percent_rank",
        "discount_rank",
        "amount",
        F.stddev_samp("discount").over(w_recent).alias("status_stddev_samp_discount"),
        F.round((F.col("amount") / F.sum("amount").over(w_recent)), 4).alias("amount_share"),
    )
)

result = (
    scored_shipments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.status").alias("status"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("e.status_stddev_samp_discount")), 2).alias("avg_status_stddev_samp_discount"),
        F.avg(F.col("e.amount_share")).alias("avg_amount_share"),
    )
)
