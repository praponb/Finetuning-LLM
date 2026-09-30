from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_country")
w2 = Window.partitionBy("country", "customer_country").orderBy(F.col("signup_date").desc(), "id", "subscription_id")
w3 = Window.partitionBy("country").orderBy("monthly_fee", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("monthly_fee", "id", "subscription_id")
w5 = Window.orderBy("monthly_fee", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

valid = (
    df_sessions
    .filter(~F.col("device").isin(["mobile", "tablet"]) & (F.col("started_at") > F.lit("2023-04-06 12:00:00").cast("timestamp")))
    .select("*", F.upper("browser").alias("browser_upper"))
)

joined = (
    valid
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.age") != 47)
    .select(
        "a.id",
        "a.customer_id",
        "a.browser",
        "a.country",
        F.col("p.signup_date"),
        F.col("p.country").alias("customer_country"),
        F.col("p.credit_limit"),
    )
)

matched = (
    joined
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & F.col("s.signup_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

enriched = (
    matched
    .withColumn("customer_country_min_subscription_id", F.min("subscription_id").over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w3))
    .withColumn("next_credit_limit", F.lead("credit_limit", 2, 0).over(w4))
    .withColumn("running_subscription_id", F.sum("subscription_id").over(w5))
)

with_last = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page"), F.col("c.duration_sec"))
)

result = (
    with_last
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .cube(F.col("c.city").alias("city"), F.col("a.plan").alias("plan"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("a.duration_sec")).alias("min_duration_sec"),
        F.max(F.col("a.seq_no")).alias("max_seq_no"),
    )
)
