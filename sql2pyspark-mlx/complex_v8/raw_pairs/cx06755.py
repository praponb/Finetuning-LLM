from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_all_by_event_at_running = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_ticket_events
    .filter(F.col("actor") == "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.when(F.col("actor").isin("customer", "system"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.lead("event_at").over(w_actor_ticket_id_by_event_at).alias("next_event_at"),
        F.row_number().over(w_actor_ticket_id_by_event_at).alias("event_at_seq"),
        F.ntile(3).over(w_all_by_event_at).alias("event_at_tercile"),
        F.last("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("last_known_event_at"),
    )
)

result = (
    metrics
    .select(
        "id",
        F.col("event_at_tercile").alias("event_at_tercile_v"),
        F.col("event_at_seq").alias("m_event_at_seq"),
    )
    .unpivot(
        ids=["id"],
        values=["event_at_tercile_v", "m_event_at_seq"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
