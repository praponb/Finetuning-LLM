from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("event_event_type").orderBy(F.col("event_event_at").desc(), F.col("id").desc(), F.col("event_id").desc())
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id", "event_id")

cleaned_events = (
    df_ticket_events
    .filter((F.month(F.col("event_at")).isin(2, 3)) | (~F.col("actor").isin("system", "agent")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
)

with_children_events = (
    cleaned_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") != "resolved"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_week",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

deduped = (
    with_children_events
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

metrics = (
    deduped
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.row_number().over(w_all_by_event_at).alias("event_at_seq"),
        F.lead("event_at_week", 3).over(w_all_by_event_at).alias("next3_event_at_week"),
        F.ntile(10).over(w_all_by_event_at).alias("event_at_decile"),
    )
)

result = (
    metrics
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_at_week",
        "event_id",
        "event_event_type",
        "event_actor",
        "event_at_dense_rank",
        "first_event_at",
        "event_at_seq",
        "next3_event_at_week",
        "event_at_decile",
        F.initcap(F.trim(F.col("event_event_type"))).alias("event_event_type_clean"),
    )
    .orderBy(F.col("event_event_type_clean").desc())
)
