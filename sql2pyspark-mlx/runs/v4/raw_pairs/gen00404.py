from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("customer_id", "promo").orderBy("pos", "id", "pos")
w3 = Window.partitionBy("customer_id").orderBy(F.col("pos").desc())
w4 = Window.partitionBy("customer_id").orderBy(F.col("pos").desc(), "id", "pos", "subscription_id")
w5 = Window.orderBy("created_at", "id", "pos", "subscription_id").rowsBetween(-6, -1)

exploded = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "item"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "pos",
        F.col("item.qty"),
        F.col("item.promo"),
    )
)

scored = (
    exploded
    .withColumn("qty_max_1h", F.max("qty").over(w))
    .withColumn("pos_bucket", F.ntile(4).over(w2))
)

deduped = (
    scored
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_window = (
    deduped
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.created_at") >= F.col("sub.start_date")) & (F.col("s.created_at") <= F.date_add(F.col("sub.end_date"), 7)), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

enriched = (
    with_window
    .withColumn("pos_bucket_2", F.ntile(3).over(w4))
    .withColumn("monthly_fee_avg_6_prior", F.avg("monthly_fee").over(w5))
)

remaining_keys = (
    enriched
    .select("customer_id", "id")
    .intersect(
        df_carts
        .filter(F.col("customer_id").isin([11, 25, 26]))
        .select("customer_id", "id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

daily = (
    verified
    .groupBy(
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
        F.when(F.col("pos_bucket_2") >= 6, "HIGH").otherwise("LOW").alias("pos_bucket_2_tier"),
        "subscription_id",
    )
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.sum(F.when(F.col("promo") == "WELCOME5", F.col("qty_max_1h")).otherwise(0)).alias("welcome5_qty_max_1h"),
        F.count(F.when(F.col("promo") == "VIP", 1)).alias("vip_count"),
        F.percentile_approx("pos_bucket_2", 0.95).alias("pos_bucket_2_p95"),
    )
    .filter(F.col("distinct_plans") < 1)
)

result = (
    daily
    .select(
        "hour_window",
        "pos_bucket_2_tier",
        "subscription_id",
        "welcome5_qty_max_1h",
        "vip_count",
        "pos_bucket_2_p95",
    )
)
