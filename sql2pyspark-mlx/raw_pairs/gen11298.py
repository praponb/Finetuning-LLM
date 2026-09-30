from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("days_since_event_time").desc())
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("country").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("event_type").orderBy("days_since_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy(F.date_trunc("week", "event_time"), "event_type")
w6 = Window.partitionBy("event_type", "session_id").orderBy("latest_rank")

valid = (
    df_web_events
    .filter(F.col("page") == "/product")
    .withColumn("days_since_event_time", F.datediff(F.current_date(), "event_time"))
    .withColumn("event_type_page", F.concat("event_type", F.lit("/"), "page"))
    .withColumn("page_clean", F.coalesce("page", F.lit("UNKNOWN")))
)

ranked = (
    valid
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

joined = (
    ranked
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "inner")
    .filter(F.col("b.browser").isin(["Chrome", "Safari"]))
    .select("src.*", F.col("b.country"))
)

scored = (
    joined
    .select(
        "*",
        F.sum("duration_sec").over(w2).alias("cumulative_duration_sec"),
        F.sum("duration_sec").over(w_running).alias("cumulative_duration_sec_2"),
        F.first("page", ignorenulls=True).over(w4).alias("first_known_page"),
        F.sum("days_since_event_time").over(w_part).alias("event_time_period_sum_days_since_event_time"),
    )
)

deduped = (
    scored
    .withColumn("latest_rank_2", F.rank().over(w6))
    .filter(F.col("latest_rank_2") == 1)
    .drop("latest_rank_2")
)

result = (
    deduped
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "days_since_event_time",
        "event_type_page",
        "latest_rank",
        "country",
    )
)
