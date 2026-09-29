from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id")
w_actor_ticket_id_by_event_at_2 = Window.partitionBy("actor", "ticket_id").orderBy("event_at")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id")
w_event_at_dense_rank_event_at = Window.partitionBy("event_at_dense_rank", "event_at_decile").orderBy(F.col("event_at_decile").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_dense_rank_actor = Window.partitionBy("event_at_dense_rank", "actor")
w_actor_by_event_at_decile_run = Window.partitionBy("actor").orderBy("event_at_decile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_rank_ties = Window.orderBy("event_at_rank")
w_event_at_dense_rank_event_at_2 = Window.partitionBy("event_at_dense_rank", "event_at_decile").orderBy(F.col("event_at_decile").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_agent_id = Window.partitionBy("ticket_agent_id")

scoped = (
    df_ticket_events
    .filter(F.col("event_type") == "created")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.initcap(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

with_details = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select("s.*", F.col("t.agent_id").alias("ticket_agent_id"))
)

with_windows = (
    with_details
    .select(
        "*",
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.lead("event_at", 2).over(w_actor_ticket_id_by_event_at).alias("next2_event_at"),
        F.rank().over(w_actor_ticket_id_by_event_at_2).alias("event_at_rank"),
        F.ntile(10).over(w_all_by_event_at).alias("event_at_decile"),
        F.round(F.percent_rank().over(w_all_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.lag("event_at").over(w_event_type_actor_by_event_at).alias("prev_event_at"),
        F.lead("event_at", 2).over(w_all_by_event_at).alias("next2_event_at_2"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.least(F.col("event_at_cume_dist"), F.col("event_at_dense_rank")).alias("least_event_at_cume_dist_event_at_dense_rank"),
        F.substring(F.trim(F.col("event_type")), 1, 2).alias("event_type_prefix"),
        F.concat(F.col("event_type_clean"), F.lit("#"), F.col("event_at_decile").cast("string")).alias("event_type_clean_ref"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.nth_value("second_event_at", 2).over(w_event_at_dense_rank_event_at).alias("second_second_event_at"),
        F.round(F.col("minutes_since_prev_event_at") / F.nullif(F.sum("minutes_since_prev_event_at").over(w_event_at_dense_rank_actor), F.lit(0)), 4).alias("minutes_since_prev_event_at_share_of_event_at_de"),
        F.round(F.col("event_at_dense_rank") / F.nullif(F.sum("event_at_dense_rank").over(w_event_at_dense_rank_actor), F.lit(0)), 4).alias("event_at_dense_rank_share_of_event_at_dense_rank"),
        F.sum("event_at_percent_rank").over(w_actor_by_event_at_decile_run).alias("running_event_at_percent_rank"),
        F.rank().over(w_all_by_event_at_rank_ties).alias("event_at_rank_rank"),
        F.min("event_at_rank").over(w_event_at_dense_rank_event_at_2).alias("min_event_at_rank_so_far"),
        F.size(F.collect_set("event_type").over(w_ticket_agent_id)).alias("distinct_event_type_in_ticket_agent_id"),
        F.round(F.col("event_at_rank") - F.avg("event_at_rank").over(w_event_at_dense_rank_actor), 2).alias("event_at_rank_vs_event_at_dense_rank_actor_avg"),
    )
)

result = (
    ranked
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_type_clean",
        "ticket_agent_id",
        "second_event_at",
        "event_at_decile",
        "event_at_percent_rank",
        "event_at_dense_rank",
        "prev_event_at",
        "least_event_at_cume_dist_event_at_dense_rank",
        "second_second_event_at",
        "minutes_since_prev_event_at_share_of_event_at_de",
        "event_at_dense_rank_share_of_event_at_dense_rank",
        "running_event_at_percent_rank",
        "event_at_rank_rank",
        "min_event_at_rank_so_far",
        "distinct_event_type_in_ticket_agent_id",
        "event_at_rank_vs_event_at_dense_rank_actor_avg",
    )
    .orderBy(
        F.col("event_at_decile").desc_nulls_first(),
        F.col("distinct_event_type_in_ticket_agent_id").desc(),
        "id",
    )
    .limit(20)
)
