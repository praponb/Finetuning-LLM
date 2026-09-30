from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id")
w_hist = Window.partitionBy("store_id").orderBy("prev_shipping_fee", "id", "subscription_id")
w_running = Window.partitionBy("customer_id")
w4 = Window.partitionBy("status", "customer_id").orderBy(F.col("monthly_fee").desc(), "id", "subscription_id")
w_ordered = Window.partitionBy("status", "customer_id").orderBy(F.col("subscription_id").desc(), "id", "subscription_id")
w6 = Window.partitionBy("status", "store_id").orderBy("next_amount", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").desc(), "id", "subscription_id")
w8 = Window.partitionBy("customer_id").orderBy("customer_id_max_monthly_fee", "id", "subscription_id")
w9 = Window.partitionBy(F.date_trunc("YEAR", "order_date"), "store_id").orderBy("next_amount", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("plan").orderBy("next_shipping_fee", "id", "subscription_id").rowsBetween(-5, -1)

cleaned = (
    df_orders
    .filter((F.col("order_date") > F.to_date(F.lit("2023-06-15"))) & F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "shipping_fee",
        F.upper("channel").alias("channel_upper"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

enriched = (
    cleaned
    .withColumn("next_amount", F.lead("amount").over(w))
    .withColumn("next_shipping_fee", F.lead("shipping_fee").over(w))
    .withColumn("prev_shipping_fee", F.lag("shipping_fee").over(w))
    .withColumn("row_seq", F.row_number().over(w))
)

attributed = (
    enriched
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.order_date") >= F.col("sub.start_date")) & (F.col("s.order_date") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

scored = (
    attributed
    .filter(F.col("subscription_id") == 5)
    .withColumn("seq_no", F.row_number().over(w_hist))
    .withColumn("customer_id_max_monthly_fee", F.max("monthly_fee").over(w_running))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("subscription_id_bucket", F.ntile(3).over(w_ordered))
    .withColumn("running_shipping_fee", F.sum("shipping_fee").over(w6))
)

not_flagged = (
    scored
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.order_date")), "left_semi")
)

scored_orders = (
    not_flagged
    .withColumn("shipping_fee_bucket", F.ntile(3).over(w7))
    .withColumn("prev_amount", F.lag("amount", 1).over(w8))
    .withColumn("running_amount", F.sum("amount").over(w9))
    .withColumn("event_seq_avg_5_prior", F.avg("event_seq").over(w10))
)

result = (
    scored_orders
    .groupBy(
        F.when(F.col("seq_no") >= 7, "PREMIUM").otherwise("STANDARD").alias("seq_no_tier"),
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.count("*").alias("event_count"),
        F.percentile_approx("customer_id_max_monthly_fee", [0.25, 0.5, 0.75]).alias("customer_id_max_monthly_fee_quartiles"),
    )
)
