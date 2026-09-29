from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy("subscription_id", "id", "subscription_id")
w4 = Window.partitionBy("country").orderBy("started_at", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

kept = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < F.col("s.started_at")), "left_semi")
)

flagged = (
    kept
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w2))

matched = (
    sessions_sessions
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.started_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

metrics = (
    matched
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "is_new_burst",
        "subscription_id",
        "plan",
        "monthly_fee",
        F.lag("is_new_burst", 1).over(w3).alias("prev_is_new_burst"),
        F.last("country", ignorenulls=True).over(w4).alias("last_known_country"),
    )
)

with_dims = (
    metrics
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .filter(~F.col("d.segment").isin(["Consumer", "SMB"]))
    .select(
        "e.id",
        "e.customer_id",
        "e.browser",
        "e.started_at",
        "e.is_new_burst",
        "e.subscription_id",
        "e.plan",
        "e.prev_is_new_burst",
        F.col("d.credit_limit"),
        F.col("d.segment"),
        F.col("d.age"),
    )
)

joined = (
    with_dims
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.started_at",
        "src.subscription_id",
        "src.prev_is_new_burst",
        "src.credit_limit",
        "src.age",
        F.col("d.email"),
    )
)

result = (
    joined
    .groupBy(
        F.when(F.col("credit_limit") >= 17700, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
    )
    .agg(
        F.max("started_at").alias("ended_at"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.min("started_at").alias("started_at"),
    )
    .filter(F.col("duration_min") > 0)
    .orderBy(F.col("credit_limit_tier").desc())
)
