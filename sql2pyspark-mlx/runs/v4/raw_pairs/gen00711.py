from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee", "id", "subscription_id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc(), "id", "subscription_id")
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id", "subscription_id")
w4 = Window.partitionBy("customer_id").orderBy("started_at", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("device").isin(["mobile", "desktop"]))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.upper("device").alias("device_upper"),
        F.length("country").alias("country_length"),
    )
)

matched = (
    base
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.started_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

enriched = (
    matched
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("row_seq", F.row_number().over(w2))
)

with_prev = enriched.withColumn("prev_started_at", F.lag("started_at").over(w3))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w4))

kept_keys = (
    sessions_sessions
    .select("country", "customer_id")
    .intersectAll(
        df_sessions
        .filter(F.col("country") == "TH")
        .select("country", "customer_id")
    )
)

verified = (
    sessions_sessions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "device_upper",
        "country_length",
        "subscription_id",
        "plan",
        F.when(F.col("plan").rlike("^(enterprise|premium)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("plan_zone"),
    )
    .orderBy("country_length")
)
