from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc(), "id")
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_all_by_event_at_ties = Window.orderBy("event_at")
w_event_at_quintile = Window.partitionBy("event_at_quintile")
w_actor_event_at_dense_rank_by = Window.partitionBy("actor", "event_at_dense_rank").orderBy("event_at_dense_rank", "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-02-24 10:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(" / ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

ranked = (
    filtered
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        F.lead("event_at").over(w_ticket_id_event_type_by_even).alias("next_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.row_number().over(w_all_by_event_at).alias("rn"),
        F.row_number().over(w_ticket_id_event_type_by_even).alias("event_at_seq"),
        F.dense_rank().over(w_ticket_id_by_event_at).alias("event_at_dense_rank"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.row_number().over(w_ticket_id_by_event_at).alias("rn_2"),
        F.ntile(5).over(w_all_by_event_at).alias("event_at_quintile"),
        F.lead("event_at").over(w_ticket_id_by_event_at).alias("next_event_at_2"),
        F.ntile(5).over(w_event_type_by_event_at).alias("event_at_quintile_2"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at_2"),
        F.lead("event_at").over(w_event_type_by_event_at).alias("next_event_at_3"),
        F.rank().over(w_event_type_by_event_at).alias("event_at_rank"),
    )
)

top_rows = (
    ranked
    .filter(F.col("rn") == 1)
    .drop("rn")
)

windowed = (
    top_rows
    .select(
        "*",
        F.sum("event_at_cume_dist").over(w_event_at_quintile).alias("event_at_quintile_sum_event_at_cume_dist"),
        F.round(F.col("event_at_quintile_2") / F.nullif(F.sum("event_at_quintile_2").over(w_event_at_quintile), F.lit(0)), 4).alias("event_at_quintile_2_share_of_event_at_quintile"),
        F.round(F.avg("event_at_rank").over(w_actor_event_at_dense_rank_by), 2).alias("event_at_rank_avg_7"),
        F.sum("event_at_dense_rank").over(w_event_at_quintile).alias("event_at_quintile_sum_event_at_dense_rank"),
        F.round(F.col("rn_2") - F.avg("rn_2").over(w_event_at_quintile), 2).alias("rn_2_vs_event_at_quintile_avg"),
    )
)

result = (
    windowed
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "next_event_at",
        "minutes_since_prev_event_at",
        "event_at_seq",
        "event_at_dense_rank",
        "event_at_cume_dist",
        "event_at_quintile",
        "next_event_at_2",
        "event_at_quintile_2",
        "minutes_since_prev_event_at_2",
        "next_event_at_3",
        "event_at_rank",
        "event_at_quintile_sum_event_at_cume_dist",
        "event_at_quintile_2_share_of_event_at_quintile",
        "event_at_rank_avg_7",
        "event_at_quintile_sum_event_at_dense_rank",
        "rn_2_vs_event_at_quintile_avg",
        F.when(F.col("actor").isin("customer", "system"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
        F.round(F.col("event_at_rank") / F.nullif(F.col("event_at_quintile_2"), F.lit(0)), 3).alias("event_at_rank_per_event_at_quintile_2"),
    )
    .orderBy(
        "event_at_quintile_2",
        F.col("event_at_rank_per_event_at_quintile_2").desc(),
    )
)
