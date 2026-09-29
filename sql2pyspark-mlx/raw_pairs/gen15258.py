from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "city").orderBy("credit_limit", "id")
w_recent = Window.partitionBy("city").orderBy("credit_limit", "id")
w3 = Window.partitionBy("segment").orderBy(F.col("prev_age").desc_nulls_last(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())

filtered = (
    df_customers
    .filter((F.col("age") == 38) & F.col("city").isNotNull())
    .withColumn("name_lower", F.lower("name"))
    .withColumn("name_length", F.length("name"))
    .withColumn("city_clean", F.coalesce("city", F.lit("DIRECT")))
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.started_at") >= F.col("s.signup_date")), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") < F.col("s.signup_date")), "left_anti")
)

enriched = (
    kept
    .filter(F.col("segment") != "Consumer")
    .select(
        "id",
        "email",
        "segment",
        "credit_limit",
        "age",
        F.lag("age", 2).over(w).alias("prev_age"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "segment", "credit_limit", "age", "event_seq")
)

valid = (
    ranked
    .filter(F.col("event_seq") <= 7)
    .select(
        "*",
        (F.col("credit_limit") + F.col("event_seq")).alias("credit_limit_plus_event_seq"),
        F.coalesce("age", F.lit(0)).alias("age_filled"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.event_seq",
        "s.credit_limit_plus_event_seq",
        F.col("c.monthly_fee").alias("subscription_monthly_fee"),
    )
)

result = (
    with_last
    .rollup(
        F.when(F.col("event_seq") >= 7, "HIGH").otherwise("LOW").alias("event_seq_tier"),
    )
    .agg(
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.percentile_approx("credit_limit_plus_event_seq", [0.25, 0.5, 0.75]).alias("credit_limit_plus_event_seq_quartiles"),
        F.percentile_approx("credit_limit_plus_event_seq", 0.5).alias("credit_limit_plus_event_seq_p50"),
    )
    .filter(F.col("credit_limit_plus_event_seq_p50") >= 0)
    .orderBy(F.col("event_seq_tier").desc_nulls_first())
)
