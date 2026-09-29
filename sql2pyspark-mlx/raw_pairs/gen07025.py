from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w_recent = Window.orderBy(F.col("shipping_fee_whole").desc_nulls_last())
w3 = Window.partitionBy("promo_code").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)

filtered = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-04-27").cast("date")) & F.col("status").isin(["PENDING", "RETURNED", "SHIPPED"]))
    .select(
        "id",
        "customer_id",
        "status",
        "channel",
        "amount",
        "discount",
        F.floor("shipping_fee").alias("shipping_fee_whole"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

base = (
    filtered
    .filter(F.col("shipping_fee_whole") != 69)
    .select(
        "id",
        "customer_id",
        "channel",
        "amount",
        "discount",
        "shipping_fee_whole",
        "channel_clean",
        F.round("shipping_fee_whole", 0).alias("shipping_fee_whole_rounded"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean_2"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.method"), F.col("c.paid_at"))
)

attributed = (
    with_latest
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.paid_at") >= F.col("pr.starts_on")) & (F.col("s.paid_at") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    attributed
    .select(
        "id",
        "customer_id",
        "shipping_fee_whole",
        "channel_clean",
        "channel_clean_2",
        "promo_id",
        F.dense_rank().over(w_recent).alias("shipping_fee_whole_dense_rank"),
        F.count("discount_pct").over(w3).alias("discount_pct_count_7d"),
    )
)
