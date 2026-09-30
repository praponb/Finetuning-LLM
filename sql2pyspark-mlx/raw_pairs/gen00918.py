from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("customer_id").orderBy("age", "id")
w2 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country", "device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("city").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

cleaned = (
    df_sessions
    .filter(F.col("browser") == "Firefox")
    .select("*", F.coalesce("country", F.lit("Unknown")).alias("country_filled"))
)

joined = (
    cleaned
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id").eqNullSafe(F.col("d.id")), "inner")
    .select(
        "e.*",
        F.col("d.age"),
        F.col("d.country").alias("customer_country"),
        F.col("d.city"),
    )
)

kept = (
    joined
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.started_at")), "left_semi")
)

metrics = (
    kept
    .withColumn("event_seq", F.row_number().over(w_recent))
    .withColumn("first_known_city", F.first("city", ignorenulls=True).over(w2))
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w3))
    .withColumn("age_avg_7d", F.avg("age").over(w_grp))
)

with_last = (
    metrics
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.device",
        "s.browser",
        "s.started_at",
        "s.age",
        "s.event_seq",
        "s.age_avg_7d",
        F.col("c.event_type"),
        F.col("c.page"),
    )
)

with_window = (
    with_last
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.started_at") >= F.col("sub.start_date")) & (F.col("a.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

daily = (
    with_window
    .groupBy("event_type", F.date_trunc("year", "started_at").alias("year_start"))
    .agg(
        F.sum(F.when(F.col("browser") == "Firefox", F.col("subscription_id"))).alias("firefox_total_subscription_id"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_count"),
        F.max(F.when(F.col("plan") == "standard", F.col("subscription_id"))).alias("standard_max_subscription_id"),
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("plan") == "premium", 1)), F.lit(0)), 2).alias("age_per_premium"),
        F.count_if(F.col("event_type") == "page_view").alias("page_view_events"),
    )
)

result = (
    daily
    .select(
        "event_type",
        "year_start",
        "firefox_total_subscription_id",
        "desktop_count",
        "age_per_premium",
        F.when(F.col("event_type").rlike("^(click|page_view)$"), "GROUP_A").when(F.col("event_type").rlike("^(add_to_cart|purchase)$"), "PRIORITY").otherwise("EXTENDED").alias("event_type_zone"),
    )
    .orderBy(F.col("event_type_zone").desc_nulls_last(), "year_start")
)
