from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("country").orderBy("event_time", "id")
w5 = Window.partitionBy("country").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("session_id", F.sum("is_new_session").over(w2))

flagged_2 = (
    sessions_sessions
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = flagged_2.withColumn("burst_id", F.sum("session_start_flag").over(w2))

latest_match = (
    islands
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time"), F.col("c.page").alias("web_event_page"))
)

joined = (
    latest_match
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .select("s.*", F.col("c.age"), F.col("c.credit_limit"), F.col("c.signup_date"))
)

gaps = (
    joined
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w4).isNull() | (F.col("event_time") > F.lag("event_time").over(w4) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id_2", F.sum("new_group_flag").over(w5))

result = (
    sessionized
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 17000, "PREMIUM").otherwise("STANDARD"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "event_time"))
    .cube("credit_limit_tier", "quarter_start")
    .agg(
        F.countDistinct("web_event_page").alias("distinct_web_event_pages"),
        F.max("started_at").alias("last_seen"),
        F.percentile_approx("session_start_flag", [0.25, 0.5, 0.75]).alias("session_start_flag_quartiles"),
        F.grouping("credit_limit_tier").alias("grouping_id"),
    )
)
