from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc(), "id")
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy("event_at")
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")

scoped = (
    df_ticket_events
    .filter(F.col("event_type") == "assigned")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("resolved", "reopened"), "group_a").when(F.col("event_type") == "created", "group_b").otherwise("other").alias("event_type_group"),
    )
)

filtered_2 = (
    scoped.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor").isin("system", "customer")), "left_semi")
)

sequenced = (
    filtered_2
    .select(
        "*",
        F.lead("event_at").over(w_ticket_id_event_type_by_even).alias("next_event_at"),
        F.dense_rank().over(w_event_type_by_event_at_ties).alias("event_at_dense_rank"),
        F.lag("event_at").over(w_event_type_by_event_at).alias("prev_event_at"),
    )
)

result = (
    sequenced
    .rollup("event_type_group", "event_type", "ticket_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.sum("event_at_dense_rank").alias("total_event_at_dense_rank"),
        F.round(F.avg("event_at_dense_rank"), 2).alias("avg_event_at_dense_rank"),
    )
    .select(
        F.coalesce(F.col("event_type_group"), F.lit("ALL")).alias("event_type_group"),
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "ticket_id",
        "gid",
        "total_event_at_dense_rank",
        "avg_event_at_dense_rank",
    )
    .orderBy(
        F.col("event_type_group").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
    )
)
