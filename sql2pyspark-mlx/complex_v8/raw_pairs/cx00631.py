from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_running = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_events = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
    )
)

sequenced = (
    prepared_events
    .select(
        "*",
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.rank().over(w_all_by_event_at).alias("event_at_rank"),
        F.first("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("first_known_event_at"),
    )
)

result = (
    sequenced
    .rollup("actor", "event_at_dense_rank")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.round(F.avg("event_at_rank"), 2).alias("avg_event_at_rank"),
        F.sum("event_at_rank").alias("total_event_at_rank"),
    )
    .select(
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        "event_at_dense_rank",
        "grouping_level",
        "distinct_actors",
        "avg_event_at_rank",
        "total_event_at_rank",
    )
    .orderBy(
        F.col("actor").asc_nulls_last(),
        F.col("event_at_dense_rank").asc_nulls_last(),
    )
)
