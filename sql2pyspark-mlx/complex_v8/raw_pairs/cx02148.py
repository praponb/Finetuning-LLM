from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_id_event_type = Window.partitionBy("session_id", "event_type")
w_prev_event_at_to_date_by_is = Window.partitionBy(F.to_date("prev_event_at")).orderBy("is_new_session", "id").rowsBetween(-2, Window.currentRow)
w_prev_event_at_to_date_by_is_2 = Window.partitionBy(F.to_date("prev_event_at")).orderBy("is_new_session", "id")
w_prev_event_at_to_date_by_is_3 = Window.partitionBy(F.to_date("prev_event_at")).orderBy("is_new_session", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = prepared.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_actor_running))

with_windows = (
    sessionized
    .select(
        "*",
        F.round(F.col("is_new_session") / F.nullif(F.sum("is_new_session").over(w_session_id_event_type), F.lit(0)), 4).alias("is_new_session_share_of_session_id_event_type"),
        F.min("is_new_session").over(w_session_id_event_type).alias("session_id_event_type_min_is_new_session"),
        F.sum("session_id").over(w_prev_event_at_to_date_by_is).alias("session_id_sum_3"),
        F.ntile(5).over(w_prev_event_at_to_date_by_is_2).alias("is_new_session_quintile"),
        F.first("session_id", ignorenulls=True).over(w_prev_event_at_to_date_by_is_3).alias("first_known_session_id"),
    )
)

scored = (
    with_windows
    .filter(F.col("event_type").isin("reopened", "replied", "assigned"))
    .select(
        "*",
        F.when(F.col("first_known_session_id") >= 5, "A").when(F.col("first_known_session_id") >= 3, "B").otherwise("C").alias("first_known_session_id_tier"),
    )
)

result = (
    scored
    .select("id", "is_new_session_quintile", "first_known_session_id")
    .unpivot(
        ids=["id"],
        values=["is_new_session_quintile", "first_known_session_id"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
