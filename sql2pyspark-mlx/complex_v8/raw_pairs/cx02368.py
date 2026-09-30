from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type")
w_all_by_event_at_year = Window.orderBy(F.col("event_at_year").desc_nulls_first(), "id")
w_actor_event_at_year_by_event = Window.partitionBy("actor", "event_at_year").orderBy("event_at", "id").rowsBetween(-2, Window.currentRow)
w_actor_event_at_year_by_event_2 = Window.partitionBy("actor", "event_at_year").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_ticket_events
    .filter(F.col("actor").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.year(F.col("event_at")).alias("event_at_year"),
    )
)

ranked = (
    cleaned
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_type_actor_label",
        "event_at_year",
        F.sum("event_at_year").over(w_event_type).alias("event_type_sum_event_at_year"),
        F.round(F.percent_rank().over(w_all_by_event_at_year), 3).alias("event_at_year_percent_rank"),
        F.max("event_at_year").over(w_actor_event_at_year_by_event).alias("event_at_year_max_3"),
        F.nth_value("event_at", 2).over(w_actor_event_at_year_by_event_2).alias("second_event_at"),
    )
)

summary = (
    ranked
    .groupBy(
        F.date_trunc("month", "second_event_at").alias("second_event_at_month"),
        "event_at_year",
        "event_type",
    )
    .agg(
        F.round(F.sum("event_at_year_percent_rank") / F.nullif(F.sum("event_at_year_max_3"), F.lit(0)), 3).alias("event_at_year_percent_rank_to_event_at_year_max_"),
        F.max("event_at_year_max_3").alias("max_event_at_year_max_3"),
    )
)

result = (
    summary
    .groupBy("event_at_year", "second_event_at_month")
    .pivot("event_type", ["reopened", "replied", "escalated"])
    .agg(F.avg("event_at_year_percent_rank_to_event_at_year_max_").alias("avg_v"), F.max("event_at_year_percent_rank_to_event_at_year_max_").alias("max_v"))
)
