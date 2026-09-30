from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy("event_at", "id")
w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at").desc())
w_actor_ticket_id_by_event_at_2 = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at").desc(), "id")

recent = (
    df_ticket_events
    .filter((F.col("actor") == "agent") & (F.col("event_type") != "replied"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("actor").isin("system", "agent"), "group_a").when(F.col("actor") == "customer", "group_b").otherwise("other").alias("actor_group"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
    )
)

windowed = (
    recent
    .select(
        "*",
        F.lag("event_at").over(w_all_by_event_at).alias("prev_event_at"),
        F.row_number().over(w_all_by_event_at).alias("event_at_seq"),
        F.round(F.percent_rank().over(w_actor_ticket_id_by_event_at), 3).alias("event_at_percent_rank"),
        F.lead("event_at", 3).over(w_actor_ticket_id_by_event_at_2).alias("next3_event_at"),
        F.lead("event_at").over(w_all_by_event_at).alias("next_event_at"),
        F.rank().over(w_actor_ticket_id_by_event_at_2).alias("event_at_rank"),
    )
)

result = (
    windowed
    .cube("event_at_seq", "event_type")
    .agg(
        F.grouping("event_at_seq").alias("is_total_event_at_seq"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.round(F.sum("event_at_seq") / F.nullif(F.sum("event_at_percent_rank"), F.lit(0)), 3).alias("event_at_seq_to_event_at_percent_rank_ratio"),
    )
    .select(
        "event_at_seq",
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "is_total_event_at_seq",
        "event_type_list",
        "event_at_seq_to_event_at_percent_rank_ratio",
    )
    .orderBy(
        F.col("event_at_seq").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
    )
)
