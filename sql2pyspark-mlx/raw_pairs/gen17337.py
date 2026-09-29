from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser").orderBy("started_at", "id", "subscription_id")
w4 = Window.partitionBy("browser").orderBy("started_at", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device").orderBy("subscription_id", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("device").orderBy("island_id", "id", "subscription_id").rowsBetween(-3, -1)
w7 = Window.partitionBy("browser").orderBy("subscription_id", "id", "subscription_id")

eligible = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.duration_sec").between(296, 474)), "left_anti")
)

flagged = (
    eligible
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_group_flag").over(w2))

matched = (
    sessionized
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.started_at") >= F.col("sub.start_date")) & (F.col("src.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

flagged_2 = (
    matched
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w3) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = (
    flagged_2
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        "new_group_flag",
        "subscription_id",
        F.sum("is_new_burst").over(w4).alias("island_id"),
    )
)

enriched = (
    islands
    .filter(F.col("island_id") > 4)
    .select(
        "*",
        F.last("subscription_id", ignorenulls=True).over(w5).alias("last_known_subscription_id"),
        F.avg("subscription_id").over(w_hist).alias("subscription_id_avg_3_prior"),
    )
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "device", "country", "new_group_flag", "subscription_id", "island_id")
)

result = (
    deduped
    .rollup(
        F.when(F.col("new_group_flag") >= 1, "PREMIUM").otherwise("STANDARD").alias("new_group_flag_tier"),
    )
    .agg(
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
        F.percentile_approx("new_group_flag", [0.25, 0.5, 0.75]).alias("new_group_flag_quartiles"),
    )
    .orderBy(F.col("new_group_flag_tier").desc())
)
