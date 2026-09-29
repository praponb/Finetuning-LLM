from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor")
w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at_year").desc(), "id")
w_event_type_ticket_id_by_even_2 = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_last_event_at_hour_event_at = Window.partitionBy("last_event_at_hour", "event_at_year").orderBy("last_event_at_hour", "id").rowsBetween(-2, -1)
w_event_at_year = Window.partitionBy(F.year("event_at"))
w_last_event_at_hour_event_at_2 = Window.partitionBy("last_event_at_hour", "event_at_year").orderBy("last_event_at_hour", "id")

prepared_events = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-01-31").cast("date")) & (F.col("ticket_id") <= 7))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.year(F.col("event_at")).alias("event_at_year"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

ranked = (
    prepared_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_at_year",
        "event_type_or_unknown",
        F.count(F.lit(1)).over(w_actor).alias("rows_in_actor"),
        F.round(F.col("event_at_year") / F.nullif(F.sum("event_at_year").over(w_actor), F.lit(0)), 4).alias("event_at_year_share_of_actor"),
        F.lead("event_at", 3).over(w_event_type_ticket_id_by_even).alias("next3_event_at"),
        F.round(F.col("event_at_year") - F.avg("event_at_year").over(w_actor), 2).alias("event_at_year_vs_actor_avg"),
        F.last("event_at").over(w_event_type_ticket_id_by_even_2).alias("last_event_at"),
    )
)

derived = (
    ranked
    .filter(F.col("event_at_year_share_of_actor") < 0.8)
    .select(
        "id",
        "event_at",
        "event_at_year",
        "rows_in_actor",
        "event_at_year_share_of_actor",
        "next3_event_at",
        "last_event_at",
        F.round((F.unix_timestamp(F.col("next3_event_at")) - F.unix_timestamp(F.col("event_at"))) / 60, 1).alias("minutes_event_at_to_next3_event_at"),
        F.hour(F.col("last_event_at")).alias("last_event_at_hour"),
    )
)

metrics = (
    derived
    .select(
        "id",
        "event_at_year",
        "next3_event_at",
        "last_event_at",
        F.round(F.avg("event_at_year_share_of_actor").over(w_last_event_at_hour_event_at), 2).alias("event_at_year_share_of_actor_avg_2_prior"),
        F.sum("event_at_year_share_of_actor").over(w_event_at_year).alias("event_at_year_sum_event_at_year_share_of_actor"),
        F.ntile(5).over(w_last_event_at_hour_event_at_2).alias("last_event_at_hour_quintile"),
        F.min("rows_in_actor").over(w_event_at_year).alias("event_at_year_min_rows_in_actor"),
        (F.col("last_event_at_hour") - F.lag("last_event_at_hour").over(w_last_event_at_hour_event_at_2)).alias("last_event_at_hour_change"),
    )
)

derived_2 = (
    metrics
    .filter(F.col("last_event_at_hour_quintile") < 6)
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("last_event_at")).alias("days_since_last_event_at"),
        F.coalesce(F.col("event_at_year"), F.lit(0)).alias("event_at_year_filled"),
        F.abs(F.col("event_at_year") - 2025).alias("event_at_year_distance"),
    )
)

tagged = (
    derived_2
    .filter(F.month(F.col("last_event_at")).isin(2, 3))
    .select(
        "*",
        F.least(F.col("event_at_year_distance"), F.col("event_at_year_filled")).alias("least_event_at_year_distance_event_at_year_fille"),
        F.abs(F.col("event_at_year_distance") - 1114).alias("event_at_year_distance_distance"),
    )
)

result = (
    tagged
    .select(
        "id",
        "event_at_year_share_of_actor_avg_2_prior",
        "last_event_at_hour_quintile",
        "event_at_year_min_rows_in_actor",
        "days_since_last_event_at",
        "event_at_year_filled",
        "event_at_year_distance_distance",
    )
    .orderBy(
        F.col("last_event_at_hour_quintile").desc(),
        F.col("event_at_year_min_rows_in_actor").desc(),
    )
)
