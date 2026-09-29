from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_month_ticket_id_by = Window.partitionBy(F.month("event_at"), "ticket_id").orderBy(F.col("event_at_hour").desc(), "id")
w_event_at_month_ticket_id_by_2 = Window.partitionBy(F.month("event_at"), "ticket_id").orderBy(F.col("event_at_hour").desc())
w_event_at_month_ticket_id_by_3 = Window.partitionBy(F.month("event_at"), "ticket_id").orderBy(F.col("event_at_hour").desc(), "id").rowsBetween(-1, -1)

valid_events = (
    df_ticket_events
    .filter((F.col("ticket_id") <= 18) & (F.col("event_type").isin("resolved", "assigned")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
        F.month(F.col("event_at")).alias("event_at_month"),
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
    )
)

with_trends = (
    valid_events
    .select(
        "*",
        F.ntile(5).over(w_event_at_month_ticket_id_by).alias("event_at_hour_quintile"),
        F.rank().over(w_event_at_month_ticket_id_by_2).alias("event_at_hour_rank"),
        F.sum("event_at_month").over(w_event_at_month_ticket_id_by_3).alias("event_at_month_sum_1_prior"),
    )
)

derived = (
    with_trends
    .filter(F.col("event_at") < F.lit("2025-02-18 11:00:00").cast("timestamp"))
    .select(
        "id",
        "actor",
        "actor_ref",
        "event_at_month",
        "event_at_hour_quintile",
        "event_at_month_sum_1_prior",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

labeled = (
    derived
    .filter(F.col("event_type_or_unknown") != "reopened")
    .select(
        "*",
        (F.floor(F.col("event_at_hour_quintile") / 2) * 2).alias("event_at_hour_quintile_bucket"),
        F.greatest(F.col("event_at_month"), F.col("event_at_hour_quintile")).alias("greatest_event_at_month_event_at_hour_quintile"),
        F.least(F.col("event_at_hour_quintile"), F.col("event_at_month")).alias("least_event_at_hour_quintile_event_at_month"),
    )
)

top_rows = (
    labeled
    .filter((F.col("event_at_hour_rank") <= 2) & (F.col("event_at_hour_quintile_bucket") < 7))
)

scored = (
    top_rows
    .select(
        "id",
        "event_at_month",
        "event_at_hour_quintile",
        "event_at_month_sum_1_prior",
        "event_type_or_unknown",
        "greatest_event_at_month_event_at_hour_quintile",
        "least_event_at_hour_quintile_event_at_month",
        F.abs(F.col("event_at_month") - 6).alias("event_at_month_distance"),
        F.length(F.trim(F.col("actor_ref"))).alias("actor_ref_len"),
    )
)

result = (
    scored
    .filter(F.col("event_at_month") >= 3)
    .select(
        "id",
        "actor_ref_len",
        (F.floor(F.col("actor_ref_len") / 2) * 2).alias("actor_ref_len_bucket"),
        F.length(F.trim(F.col("event_type_or_unknown"))).alias("event_type_or_unknown_len"),
    )
    .orderBy(F.col("id").desc(), F.col("actor_ref_len").desc_nulls_first())
)
