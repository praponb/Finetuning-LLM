from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc(), "id")

cleaned = (
    df_orders
    .filter(F.col("shipping_fee") > 27)
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.concat("channel", F.lit(", "), "status").alias("channel_status"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.paid_at") >= F.col("s.order_date")), "left_anti")
)

deduped = (
    eligible
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "customer_id", "discount", "channel_status", "resolved_channel")
)

kept_keys = (
    deduped
    .select("customer_id")
    .subtract(
        df_orders
        .filter(F.col("shipping_fee").isNull())
        .select("customer_id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

summary = (
    kept_rows
    .withColumn("discount_tier", F.when(F.col("discount") >= 0.31, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["customer_id", "discount_tier"], ["customer_id"], []],
        "customer_id",
        "discount_tier",
    )
    .agg(
        F.stddev_samp("discount").alias("discount_stddev"),
        F.percentile_approx("discount", 0.5).alias("discount_p50"),
    )
)

result = (
    summary
    .select("customer_id")
    .orderBy(F.col("customer_id").desc_nulls_last())
)
