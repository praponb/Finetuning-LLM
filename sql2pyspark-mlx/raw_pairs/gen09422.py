from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("shipping_fee").desc_nulls_first())
w2 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("amount").desc())
w4 = Window.partitionBy("method", "order_id")

remaining_keys = (
    df_payments
    .select("order_id", "id")
    .subtract(
        df_payments
        .filter(F.hour("paid_at").between(11, 16))
        .select("order_id", "id")
    )
)

reconciled = (
    df_payments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id").eqNullSafe(F.col("d.id")), "inner")
    .select("src.*", F.col("d.shipping_fee"))
)

enriched = (
    with_dims
    .filter(F.col("method").isin(["card", "bank_transfer"]))
    .select(
        "id",
        "order_id",
        F.rank().over(w).alias("shipping_fee_rank"),
        F.sum("amount").over(w2).alias("amount_sum_30d"),
        F.percent_rank().over(w3).alias("amount_percent_rank"),
        F.stddev_samp("shipping_fee").over(w4).alias("method_stddev_samp_shipping_fee"),
    )
)

cleaned = (
    enriched
    .filter(F.col("order_id").isNotNull())
    .withColumn("shipping_fee_rank_per_amount_percent_rank", (F.col("shipping_fee_rank") / F.nullif("amount_percent_rank", F.lit(0))))
)

cleaned_payments = (
    cleaned
    .filter(F.col("order_id").isNotNull())
    .withColumn("shipping_fee_rank_per_amount_percent_rank_minus_shipping_fee_rank", (F.col("shipping_fee_rank_per_amount_percent_rank") - F.col("shipping_fee_rank")))
)

result = (
    cleaned_payments
    .groupBy("order_id")
    .agg(
        F.percentile_approx("amount_percent_rank", [0.25, 0.5, 0.75]).alias("amount_percent_rank_quartiles"),
        F.min("shipping_fee_rank").alias("min_shipping_fee_rank"),
        F.percentile_approx("shipping_fee_rank", 0.5).alias("shipping_fee_rank_p50"),
        F.min("amount_sum_30d").alias("min_amount_sum_30d"),
    )
    .filter(F.col("min_shipping_fee_rank") > 0)
)
