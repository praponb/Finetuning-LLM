from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at_runni = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc_nulls_first(), "id")
w_actor_event_type_by_event_at_2 = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc_nulls_first())
w_first_known_event_at_week_mo = Window.partitionBy(F.month("first_known_event_at_week"), "ticket_id").orderBy(F.col("event_at_dense_rank").desc(), "id")
w_event_at_dense_rank_by_event = Window.partitionBy("event_at_dense_rank").orderBy(F.col("event_at_dense_rank").desc(), "id")

valid = (
    df_ticket_events
    .filter(F.col("event_type") != "reopened")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "ticket_id",
        "actor",
        "event_at_week",
        F.first("event_at_week", ignorenulls=True).over(w_event_type_by_event_at_runni).alias("first_known_event_at_week"),
        F.lead("event_at").over(w_actor_event_type_by_event_at).alias("next_event_at"),
        F.dense_rank().over(w_actor_event_type_by_event_at_2).alias("event_at_dense_rank"),
    )
)

sequenced = (
    metrics
    .withColumn("prev_first_known_event_at_week", F.lag("first_known_event_at_week").over(w_first_known_event_at_week_mo))
    .withColumn("seq_no", F.row_number().over(w_event_at_dense_rank_by_event))
)

derived = (
    sequenced
    .select(
        "id",
        "ticket_id",
        "actor",
        "event_at_week",
        "first_known_event_at_week",
        "next_event_at",
        "event_at_dense_rank",
        "prev_first_known_event_at_week",
        "seq_no",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

result = (
    derived
    .groupBy("actor", "next_event_at")
    .pivot("actor_or_unknown", ["UNKNOWN", "customer"])
    .agg(F.min("event_at_dense_rank"))
    .orderBy("actor", "next_event_at")
)
