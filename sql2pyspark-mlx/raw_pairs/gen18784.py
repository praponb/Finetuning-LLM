from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("browser").orderBy(F.col("web_event_event_time").asc_nulls_first())
w3 = Window.partitionBy("device").orderBy(F.col("consumer_events").desc())

latest_match = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time").alias("web_event_event_time"))
)

cleaned = (
    latest_match
    .filter(F.col("browser").isNotNull() & (F.col("device") == "mobile"))
    .withColumn("started_at_period", F.date_trunc("MONTH", "started_at"))
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    ranked
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .filter(F.year(F.col("b.signup_date")) >= 2023)
    .select("e.*", F.col("b.credit_limit"), F.col("b.city"), F.col("b.segment"))
)

result = (
    joined
    .groupBy(
        "device",
        F.when(F.col("credit_limit") >= 13000, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
    )
    .agg(
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.count_if(F.col("segment") == "Consumer").alias("consumer_events"),
    )
    .withColumn("consumer_events_percent_rank", F.percent_rank().over(w3))
    .orderBy(F.col("num_segments").desc_nulls_last())
)
