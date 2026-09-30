from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy(F.col("event_at").desc())
w_event_at_month_by_event_at_f = Window.partitionBy(F.month("event_at")).orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_month_by_event_at_t = Window.partitionBy(F.month("event_at")).orderBy(F.col("event_at").asc_nulls_last())
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_event_at_runni = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

metrics = (
    prepared_events
    .select(
        "*",
        F.round(F.percent_rank().over(w_event_type_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.last("event_at").over(w_event_at_month_by_event_at_f).alias("last_event_at"),
        F.round(F.percent_rank().over(w_event_at_month_by_event_at_t), 3).alias("event_at_percent_rank_2"),
        F.last("event_at").over(w_event_type_actor_by_event_at).alias("last_event_at_2"),
        F.nth_value("event_at", 2).over(w_event_type_by_event_at_full).alias("second_event_at"),
        F.first("event_at", ignorenulls=True).over(w_event_type_by_event_at_runni).alias("first_known_event_at"),
    )
)

result = (
    metrics
    .groupingSets(
        [["actor", "event_type", "ticket_id"], ["actor"], ["event_type"], []],
        "actor", "event_type", "ticket_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.max("event_at_percent_rank_2").alias("max_event_at_percent_rank_2"),
        F.min("event_at_percent_rank_2").alias("min_event_at_percent_rank_2"),
        F.count("*").alias("row_count"),
    )
    .select(
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "ticket_id",
        "grouping_level",
        "max_event_at_percent_rank_2",
        "min_event_at_percent_rank_2",
        "row_count",
    )
)
