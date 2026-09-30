from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id")
w_event_type_actor_by_event_at_2 = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at = Window.orderBy(F.col("event_at").desc_nulls_first(), "id")
w_event_type_actor_by_event_at_3 = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc())

valid_events = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 10)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref"),
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
    )
)

with_trends = (
    valid_events
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_at_label",
        F.lead("event_at").over(w_event_type_actor_by_event_at).alias("next_event_at"),
        F.first("event_at", ignorenulls=True).over(w_event_type_actor_by_event_at_2).alias("first_known_event_at"),
        F.lead("event_at").over(w_all_by_event_at).alias("next_event_at_2"),
        F.dense_rank().over(w_event_type_actor_by_event_at_3).alias("event_at_dense_rank"),
        F.ntile(3).over(w_all_by_event_at).alias("event_at_tercile"),
    )
)

without_matches = (
    with_trends
    .crossJoin(with_trends.agg(F.avg("event_at_dense_rank").alias("overall_avg_event_at_dense_rank")))
    .filter(F.col("event_at_dense_rank") > F.col("overall_avg_event_at_dense_rank"))
    .drop("overall_avg_event_at_dense_rank")
)

result = (
    without_matches
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_at_label",
        "next_event_at_2",
    )
    .orderBy(F.col("event_at").desc())
)
