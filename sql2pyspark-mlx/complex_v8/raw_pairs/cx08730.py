from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_listener_signup_date = Window.orderBy("listener_signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_listener_signup_date_2 = Window.orderBy("listener_signup_date")
w_all_by_listener_signup_date_3 = Window.orderBy("listener_signup_date", "id")

prepared = (
    df_plays
    .filter(F.col("source").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
    )
)

enriched = (
    prepared.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.country").alias("listener_country"),
        F.col("l.plan").alias("listener_plan"),
    )
)

tagged = (
    enriched
    .filter(F.col("track_id").isNotNull())
    .select(
        "*",
        F.when(F.col("listener_plan").isin("premium", "free"), "group_a").when(F.col("listener_plan") == "student", "group_b").otherwise("other").alias("listener_plan_group"),
        F.substring(F.trim(F.col("listener_country")), 1, 4).alias("listener_country_prefix"),
        F.when(F.col("listener_country").isin("JP", "UK"), "group_a").when(F.col("listener_country") == "TH", "group_b").otherwise("other").alias("listener_country_group"),
    )
)

with_trends = (
    tagged
    .select(
        "*",
        F.nth_value("listener_signup_date", 2).over(w_all_by_listener_signup_date).alias("second_listener_signup_date"),
        F.round(F.cume_dist().over(w_all_by_listener_signup_date_2), 3).alias("listener_signup_date_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_listener_signup_date_2), 3).alias("listener_signup_date_percent_rank"),
        F.last("listener_signup_date").over(w_all_by_listener_signup_date).alias("last_listener_signup_date"),
        F.datediff(F.col("listener_signup_date"), F.lag("listener_signup_date").over(w_all_by_listener_signup_date_3)).alias("days_since_prev_listener_signup_date"),
    )
)

scored = (
    with_trends
    .select(
        "*",
        F.length(F.trim(F.col("source"))).alias("source_len"),
        F.date_format(F.col("listener_signup_date"), "yyyy-MM").alias("listener_signup_date_label"),
        F.date_format(F.col("listener_signup_date"), "yyyy-MM").alias("listener_signup_date_label_2"),
    )
)

result = (
    scored
    .select("id", "source_len", "days_since_prev_listener_signup_date")
    .unpivot(
        ids=["id"],
        values=["source_len", "days_since_prev_listener_signup_date"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
