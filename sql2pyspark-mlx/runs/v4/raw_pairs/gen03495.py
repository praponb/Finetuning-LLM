from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("started_at", "id")
w5 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("country").orderBy(F.col("session_id_2").desc())
w7 = Window.partitionBy("country").orderBy(F.col("subscription_id").desc(), "id", "subscription_id")

with_last = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page"))
)

gaps = (
    with_last
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w2) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_id", F.sum("new_group_flag").over(w3))

flagged = (
    sessions_sessions
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w4) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_sessions_2 = flagged.withColumn("session_id_2", F.sum("is_new_burst").over(w5))

attributed = (
    sessions_sessions_2
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.started_at") >= F.col("sub.start_date")) & (F.col("s.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

joined = (
    attributed
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.city"))
)

enriched = (
    joined
    .withColumn("session_id_2_rank", F.rank().over(w6))
    .withColumn("subscription_id_bucket", F.ntile(5).over(w7))
)

result = (
    enriched
    .withColumn("week_start", F.date_trunc("week", "started_at"))
    .withColumn("year_start", F.date_trunc("year", "started_at"))
    .withColumn("session_id_tier", F.when(F.col("session_id") >= 7, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["week_start", "year_start", "session_id_tier"], ["week_start", "year_start"]],
        "week_start",
        "year_start",
        "session_id_tier",
    )
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.max("started_at").alias("ended_at"),
        F.grouping("week_start").alias("grouping_level"),
    )
    .orderBy(F.col("year_start").desc())
)
