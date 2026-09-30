from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("order_date", "id")
w2 = Window.partitionBy("status", "store_id")
w3 = Window.partitionBy("channel")
w4 = Window.orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("promo_code", "status").orderBy(F.col("discount_plus_amount").desc(), "id", "promo_id")
w6 = Window.partitionBy("channel", "store_id")
w7 = Window.orderBy("discount_plus_amount", "id", "promo_id")
w_hist = Window.partitionBy("channel").orderBy("discount_pct", "id", "promo_id")

filtered = (
    df_orders
    .filter(F.col("amount") < 920)
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        (F.col("discount") + F.col("amount")).alias("discount_plus_amount"),
    )
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    latest
    .withColumn("status_stddev_samp_discount", F.stddev_samp("discount").over(w2))
    .withColumn("channel_count_shipping_fee", F.count("shipping_fee").over(w3))
    .withColumn("cumulative_discount_plus_amount", F.sum("discount_plus_amount").over(w4))
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w4))
)

attributed = (
    metrics
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & F.col("src.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

scored = (
    attributed
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("channel_count_shipping_fee_share", F.round((F.col("channel_count_shipping_fee") / F.sum("channel_count_shipping_fee").over(w6)), 4))
    .withColumn("seq_no_2", F.row_number().over(w7))
    .withColumn("next_cumulative_discount_plus_amount", F.lead("cumulative_discount_plus_amount", 1).over(w_hist))
)

attributed_orders = (
    scored
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.order_date") >= F.col("pr.starts_on")) & (F.col("e.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

totals = (
    attributed_orders
    .withColumn("discount_plus_amount_tier", F.when(F.col("discount_plus_amount") >= 74, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["promo_code", "discount_plus_amount_tier"], ["promo_code"]],
        "promo_code",
        "discount_plus_amount_tier",
    )
    .agg(
        F.round(F.sum("seq_no_2") / F.nullif(F.count(F.when(F.col("status") == "RETURNED", 1)), F.lit(0)), 2).alias("seq_no_2_per_returned"),
        F.count("*").alias("num_rows"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = (
    totals
    .select("promo_code", "discount_plus_amount_tier", "num_rows", "grouping_id")
    .orderBy(F.col("grouping_id").desc(), "discount_plus_amount_tier")
)
