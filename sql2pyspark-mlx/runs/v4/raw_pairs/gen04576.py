from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("subscription_id").asc_nulls_last())
w2 = Window.partitionBy("plan").orderBy("monthly_fee", "id", "subscription_id")
w3 = Window.partitionBy("plan", "customer_id").orderBy(F.col("monthly_fee").desc_nulls_last())

with_dims = (
    df_sessions
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.name"), F.col("c.segment"), F.col("c.credit_limit"))
)

with_window = (
    with_dims
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.started_at") >= F.col("sub.start_date")) & (F.col("a.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

base = (
    with_window
    .filter(F.col("plan").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "name",
        "credit_limit",
        "subscription_id",
        "plan",
        "monthly_fee",
        (F.col("credit_limit") - F.col("subscription_id")).alias("credit_limit_minus_subscription_id"),
        F.date_trunc("WEEK", "started_at").alias("started_at_period"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.rank().over(w).alias("subscription_id_rank"),
        F.row_number().over(w2).alias("event_seq"),
        F.cume_dist().over(w3).alias("monthly_fee_cume_dist"),
    )
)

kept_keys = (
    metrics
    .select("customer_id", "device")
    .exceptAll(
        df_sessions
        .filter(F.col("country").isNull())
        .select("customer_id", "device")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.segment"))
)

summary = (
    joined
    .groupBy("device")
    .agg(
        F.sum(F.when(F.col("device") == "desktop", F.col("subscription_id"))).alias("desktop_total_subscription_id"),
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
        F.percentile_approx("credit_limit", 0.5).alias("credit_limit_p50"),
        F.count("*").alias("row_count"),
        F.count_if(F.col("segment") == "Consumer").alias("consumer_events"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
    )
    .filter(F.col("desktop_total_subscription_id") >= 0)
)

result = (
    summary
    .select(
        "device",
        "desktop_total_subscription_id",
        "monthly_fee_quartiles",
        "credit_limit_p50",
        "row_count",
    )
)
