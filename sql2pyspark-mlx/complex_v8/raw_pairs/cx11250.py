from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy("event_at", "id")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_2 = Window.orderBy(F.col("event_at").desc(), "id")
w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id")
w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id")
w_all_by_event_at_running = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_ticket_events
    .filter((F.col("event_type").isin("escalated", "assigned")) & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

sequenced = (
    base
    .select(
        "id",
        "ticket_id",
        "actor",
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at"),
        F.nth_value("event_at", 2).over(w_actor_by_event_at_full).alias("second_event_at_2"),
        F.ntile(5).over(w_all_by_event_at_2).alias("event_at_quintile"),
        F.ntile(4).over(w_actor_by_event_at).alias("event_at_quartile"),
        F.lead("event_at", 3).over(w_actor_ticket_id_by_event_at).alias("next3_event_at"),
        F.last("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("last_known_event_at"),
        F.ntile(4).over(w_all_by_event_at).alias("event_at_quartile_2"),
        F.dense_rank().over(w_all_by_event_at_2).alias("event_at_dense_rank"),
    )
)

top_rows = (
    sequenced
    .filter(F.col("event_at_dense_rank") <= 2)
    .drop("event_at_dense_rank")
)

result = (
    top_rows
    .select(
        "id",
        "ticket_id",
        "actor",
        "minutes_since_prev_event_at",
        "second_event_at",
        "second_event_at_2",
        "event_at_quintile",
        "event_at_quartile",
        "next3_event_at",
        "event_at_quartile_2",
        F.when(F.col("event_at_quartile") >= 6, "HIGH").when(F.col("event_at_quartile") >= 5, "MEDIUM").otherwise("LOW").alias("event_at_quartile_tier"),
        F.round((F.unix_timestamp(F.col("second_event_at_2")) - F.unix_timestamp(F.col("next3_event_at"))) / 3600, 1).alias("hours_next3_event_at_to_second_event_at_2"),
    )
    .orderBy(F.col("actor").desc_nulls_first(), F.col("id").desc())
    .limit(5)
)
