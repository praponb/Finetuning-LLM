from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w_part = Window.orderBy(F.col("shipping_fee_per_discount").desc())
w3 = Window.partitionBy("status", "store_id")

base = (
    df_orders
    .filter((F.col("order_date") >= F.to_date(F.lit("2024-02-15"))) & F.col("customer_id").isNotNull())
    .withColumn("shipping_fee_per_discount", (F.col("shipping_fee") / F.nullif("discount", F.lit(0))))
)

remaining_keys = (
    base
    .select("customer_id", "channel")
    .subtract(
        df_orders
        .filter(F.col("amount") > 1060)
        .select("customer_id", "channel")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .withColumn("status_stddev_samp_amount", F.stddev_samp("amount").over(w))
    .withColumn("shipping_fee_per_discount_percent_rank", F.percent_rank().over(w_part))
    .withColumn("status_count_discount", F.count("discount").over(w3))
)

result = (
    metrics
    .withColumn("week_start", F.date_trunc("WEEK", "order_date"))
    .withColumn("status_stddev_samp_amount_tier", F.when(F.col("status_stddev_samp_amount") >= 1226, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["week_start", "status_stddev_samp_amount_tier"], ["week_start"], []],
        "week_start",
        "status_stddev_samp_amount_tier",
    )
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.count("*").alias("row_count"),
        F.percentile_approx("shipping_fee_per_discount_percent_rank", [0.25, 0.5, 0.75]).alias("shipping_fee_per_discount_percent_rank_quartiles"),
        F.grouping("week_start").alias("grouping_id"),
    )
    .filter(F.col("distinct_store_ids") > 1)
)
