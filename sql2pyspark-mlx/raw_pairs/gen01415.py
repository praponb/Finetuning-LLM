from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").asc_nulls_first())
w2 = Window.partitionBy("channel")
w3 = Window.partitionBy("customer_id")

base = (
    df_orders
    .filter((F.col("shipping_fee") <= 35) & (F.col("order_date") >= F.lit("2023-08-19").cast("date")))
    .select(
        "*",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

remaining_keys = (
    base
    .select("status")
    .exceptAll(
        df_orders
        .filter(F.col("customer_id") == 3)
        .select("status")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .withColumn("shipping_fee_cume_dist", F.cume_dist().over(w))
    .withColumn("channel_max_amount", F.max("amount").over(w2))
    .withColumn("customer_id_min_shipping_fee", F.min("shipping_fee").over(w3))
)

valid = (
    metrics
    .filter((F.col("order_date") >= F.lit("2023-05-20").cast("date")) & (~F.col("channel").eqNullSafe(F.lit("store"))))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount_filled",
        "channel_clean",
        "shipping_fee_cume_dist",
        "channel_max_amount",
        F.round((F.col("shipping_fee") * 1.07), 2).alias("shipping_fee_scaled"),
    )
)

result = (
    valid
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.percentile_approx("shipping_fee_cume_dist", [0.25, 0.5, 0.75]).alias("shipping_fee_cume_dist_quartiles"),
        F.percentile_approx("amount_filled", 0.9).alias("amount_filled_p90"),
    )
    .orderBy("amount_filled_p90")
)
