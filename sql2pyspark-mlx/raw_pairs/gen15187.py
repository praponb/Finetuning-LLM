from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id", "subscription_id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser").orderBy("is_new_burst", "id", "subscription_id")
w4 = Window.partitionBy("customer_id", "device")
w5 = Window.partitionBy("device").orderBy(F.col("prev_started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

base = (
    df_sessions
    .filter((F.col("started_at") >= F.to_timestamp(F.lit("2023-03-30 12:00:00"))) & (F.col("started_at") < "2023-11-01"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.substring("country", 1, 4).alias("country_prefix"),
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"),
    )
)

matched = (
    base
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.started_at") >= F.col("sub.start_date")) & (F.col("e.started_at") <= F.date_add(F.col("sub.end_date"), 3)), "inner")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_prev = matched.withColumn("prev_started_at", F.lag("started_at").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

not_flagged = (
    sessionized
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.page").isin(["/checkout", "/cart"])), "left_semi")
)

enriched = (
    not_flagged
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "started_at_label",
        "subscription_id",
        "monthly_fee",
        "session_id",
        F.row_number().over(w3).alias("row_seq"),
        F.avg("is_new_burst").over(w4).alias("customer_id_avg_is_new_burst"),
        F.avg("subscription_id").over(w5).alias("subscription_id_avg_30d"),
    )
)

result = (
    enriched
    .select(
        "id",
        "browser",
        "started_at",
        "started_at_label",
        "subscription_id",
        "monthly_fee",
        "session_id",
        "subscription_id_avg_30d",
        F.when(F.col("browser").rlike("^(Edge|Firefox)$"), "CORE").when(F.col("browser").rlike("^(Chrome|Safari)$"), "GROUP_A").otherwise("EXTENDED").alias("browser_zone"),
    )
    .orderBy(F.col("subscription_id").desc_nulls_last())
)
