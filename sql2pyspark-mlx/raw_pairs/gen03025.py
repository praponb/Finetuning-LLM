from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

filtered = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & (F.col("country").eqNullSafe(F.lit("SG"))))
    .withColumn("clean_device", F.trim("device"))
    .withColumn("started_at_period", F.date_trunc("WEEK", "started_at"))
)

filtered_sessions = (
    filtered
    .filter((F.col("started_at") >= F.lit("2023-06-12 05:00:00").cast("timestamp")) & (F.col("country") != "US"))
    .select(
        "*",
        F.date_format("started_at_period", "yyyy-MM").alias("started_at_period_label"),
    )
)

not_flagged = (
    filtered_sessions
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_type").isin(["click", "purchase"])), "left_semi")
)

joined = (
    not_flagged
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.age"), F.col("b.signup_date"))
)

latest_match = (
    joined
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec"), F.col("c.event_type"))
)

matched_keys = (
    latest_match
    .select("customer_id")
    .intersectAll(
        df_sessions
        .filter(F.col("browser").isin(["Firefox", "Chrome", "Safari"]))
        .select("customer_id")
    )
)

kept_rows = (
    latest_match
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

not_flagged_sessions = (
    kept_rows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.signup_date")), "left_anti")
)

result = (
    not_flagged_sessions
    .select(
        "browser",
        "started_at",
        "country",
        "started_at_period",
        "started_at_period_label",
        "age",
        "signup_date",
        "duration_sec",
        "event_type",
    )
)
