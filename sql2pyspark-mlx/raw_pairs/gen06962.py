from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("amount", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("shipping_fee", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy(F.col("promo_id").desc(), "id", "promo_id")
w4 = Window.partitionBy("status", "customer_id").orderBy(F.col("running_discount").asc_nulls_first(), "id", "promo_id")

with_window = (
    df_orders
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.order_date") >= F.col("pr.starts_on")) & (F.col("e.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

metrics = (
    with_window
    .filter(~F.col("status").isin(["RETURNED", "PENDING", "SHIPPED"]))
    .select(
        "*",
        F.sum("shipping_fee").over(w_running).alias("running_shipping_fee"),
        F.sum("discount").over(w2).alias("running_discount"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

kept_keys = (
    ranked
    .select("status", "channel")
    .intersectAll(
        df_orders
        .filter(F.col("status") == "COMPLETED")
        .select("status", "channel")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("channel").isNotNull() & (F.col("running_shipping_fee") >= 184))
    .select(
        "*",
        F.coalesce("shipping_fee", F.lit(0)).alias("shipping_fee_filled"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

result = (
    filtered
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        "promo_id",
        F.when(F.col("status").rlike("^(RETURNED|SHIPPED)$"), "GROUP_B").otherwise("PRIORITY").alias("status_zone"),
    )
    .orderBy(F.col("order_date").asc_nulls_last())
)
