from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_all_by_event_at = Window.orderBy(F.col("event_at").desc_nulls_first(), "id")
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_2 = Window.orderBy("event_at", "id")

cleaned = (
    df_ticket_events
    .filter((F.col("event_type").isin("escalated", "replied")) & (F.hour(F.col("event_at")).between(9, 13)))
)

ranked = (
    cleaned
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
        F.row_number().over(w_all_by_event_at).alias("rn"),
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.row_number().over(w_all_by_event_at_2).alias("event_at_seq"),
        F.lead("event_at", 3).over(w_all_by_event_at_2).alias("next3_event_at"),
    )
)

qualified = (
    ranked
    .crossJoin(ranked.agg(F.min("event_at_rank").alias("overall_min_event_at_rank")))
    .filter(F.col("event_at_rank") > F.col("overall_min_event_at_rank"))
    .drop("overall_min_event_at_rank")
)

result = (
    qualified
    .groupingSets(
        [["event_at_seq", "actor"], ["event_at_seq"], []],
        "event_at_seq", "actor",
    )
    .agg(
        F.sum(F.when(F.col("actor") == "customer", F.col("event_at_seq"))).alias("event_at_seq_actor_customer"),
        F.sum(F.when(F.col("event_type").isin("assigned", "escalated"), 1).otherwise(0)).alias("n_event_type_in_assigned_escal"),
    )
    .orderBy(F.col("event_at_seq").asc_nulls_last(), F.col("actor").asc_nulls_last())
)
