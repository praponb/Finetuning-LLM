from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc(), "id")

filtered_events = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-19 11:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
        F.initcap(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

ranked = (
    filtered_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_at_label",
        "event_type_clean",
        F.first("event_at", ignorenulls=True).over(w_ticket_id_actor_by_event_at).alias("first_known_event_at"),
        F.lag("event_at", 3).over(w_ticket_id_actor_by_event_at_2).alias("prev3_event_at"),
    )
)

result = (
    ranked
    .filter(F.col("event_type") == "assigned")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type_clean",
        "first_known_event_at",
        "prev3_event_at",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
    .orderBy(
        F.col("event_at").desc_nulls_first(),
        F.col("event_type_len").asc_nulls_last(),
    )
)
