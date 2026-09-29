from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_actor_by_event_at_2 = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id")
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")

cleaned_events = (
    df_ticket_events
    .filter((F.col("event_type").isin("resolved", "escalated")) & (F.col("actor") == "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref_2"),
    )
)

sequenced = (
    cleaned_events
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "actor_ref",
        F.nth_value("event_at", 2).over(w_event_type_actor_by_event_at).alias("second_event_at"),
        F.row_number().over(w_event_type_actor_by_event_at_2).alias("event_at_seq"),
        F.round(F.cume_dist().over(w_actor_by_event_at), 3).alias("event_at_cume_dist"),
        F.dense_rank().over(w_actor_by_event_at).alias("event_at_dense_rank"),
    )
)

enriched = (
    sequenced
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("second_event_at")) - F.unix_timestamp(F.col("event_at"))) / 3600, 1).alias("hours_event_at_to_second_event_at"),
        F.round(F.col("event_at_dense_rank") / F.nullif(F.col("event_at_cume_dist"), F.lit(0)), 3).alias("event_at_dense_rank_per_event_at_cume_dist"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

result = (
    enriched
    .select(
        "id",
        "event_at",
        "actor",
        "actor_ref",
        "event_at_seq",
        "event_at_cume_dist",
        "event_at_dense_rank",
        "hours_event_at_to_second_event_at",
        "event_at_dense_rank_per_event_at_cume_dist",
        "event_type_slug",
    )
    .orderBy(F.col("event_at_dense_rank_per_event_at_cume_dist").desc())
)
