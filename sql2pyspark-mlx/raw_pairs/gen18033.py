from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

valid = (
    df_sessions
    .filter((F.col("started_at") >= F.to_date(F.lit("2023-03-06"))) & F.col("customer_id").isNotNull())
    .withColumn("country_prefix", F.substring("country", 1, 3))
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_latest = (
    deduped
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.event_type").alias("web_event_event_type"),
        F.col("c.event_time").alias("web_event_event_time"),
    )
)

joined = (
    with_latest
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.age") <= 44)
    .select(
        "e.id",
        "e.customer_id",
        "e.browser",
        "e.started_at",
        "e.web_event_event_type",
        F.col("c.city"),
        F.col("c.email"),
        F.col("c.signup_date"),
    )
)

matched_keys = (
    joined
    .select("browser")
    .intersectAll(
        df_sessions
        .filter(F.col("device") == "tablet")
        .select("browser")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "src.customer_id",
        "src.web_event_event_type",
        "src.city",
        "src.signup_date",
        F.col("p.credit_limit"),
        F.col("p.city").alias("customer_city"),
        F.col("p.age"),
    )
)

result = (
    enriched
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "city",
        "signup_date",
        "credit_limit",
        F.when(F.col("city").rlike("^(Chiang Mai|Bangkok)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("city_zone"),
        F.col("min_subscription_monthly_fee"),
    )
)
