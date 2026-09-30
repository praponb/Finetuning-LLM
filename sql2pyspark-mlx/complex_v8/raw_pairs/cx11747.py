from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc(), "id")
w_ticket_id = Window.partitionBy("ticket_id")
w_event_type_len_ticket_id = Window.partitionBy("event_type_len", "ticket_id")
w_ticket_id_event_type_by_even_2 = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at_plus_7d").desc(), "id")
w_ticket_id_event_type_by_even_3 = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc())
w_all_by_event_at_ties = Window.orderBy("event_at")
w_actor_by_event_at_plus_7d_ru = Window.partitionBy("actor").orderBy(F.col("event_at_plus_7d").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_event_type_by_even_4 = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at_plus_7d").desc(), "id").rowsBetween(-3, Window.currentRow)
w_actor_by_event_at_plus_7d = Window.partitionBy("actor").orderBy(F.col("event_at_plus_7d").desc_nulls_first(), "id")
w_event_at_dense_rank_by_event = Window.partitionBy("event_at_dense_rank").orderBy("event_type_len_sum_4", "id").rowsBetween(-6, -1)
w_event_at_seq = Window.partitionBy("event_at_seq")

cleaned_events = (
    df_ticket_events
    .filter((~F.col("event_type").isin("created", "escalated")) & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 7 DAYS")).alias("event_at_plus_7d"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

windowed = (
    cleaned_events
    .select(
        "*",
        F.row_number().over(w_ticket_id_event_type_by_even).alias("event_at_seq"),
        F.count(F.lit(1)).over(w_ticket_id).alias("rows_in_ticket_id"),
        F.size(F.collect_set("event_type").over(w_event_type_len_ticket_id)).alias("distinct_event_type_in_event_type_len_ticket_id"),
        F.lead("event_type_len").over(w_ticket_id_event_type_by_even_2).alias("next_event_type_len"),
        F.dense_rank().over(w_ticket_id_event_type_by_even_3).alias("event_at_dense_rank"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank_2"),
        F.sum("event_type_len").over(w_actor_by_event_at_plus_7d_ru).alias("running_event_type_len"),
        F.sum("event_type_len").over(w_ticket_id_event_type_by_even_4).alias("event_type_len_sum_4"),
        F.max("event_type_len").over(w_ticket_id).alias("ticket_id_max_event_type_len"),
        F.lag("event_type_len").over(w_actor_by_event_at_plus_7d).alias("prev_event_type_len"),
        F.round(F.col("event_type_len") / F.nullif(F.sum("event_type_len").over(w_ticket_id), F.lit(0)), 4).alias("event_type_len_share_of_ticket_id"),
        F.size(F.collect_set("actor").over(w_event_type_len_ticket_id)).alias("distinct_actor_in_event_type_len_ticket_id"),
    )
)

ranked = (
    windowed
    .select(
        "*",
        F.sum("event_at_dense_rank").over(w_event_at_dense_rank_by_event).alias("event_at_dense_rank_sum_6_prior"),
        F.round(F.col("running_event_type_len") - F.avg("running_event_type_len").over(w_event_at_seq), 2).alias("running_event_type_len_vs_event_at_seq_avg"),
        F.round(F.col("event_at_dense_rank") / F.nullif(F.sum("event_at_dense_rank").over(w_event_at_seq), F.lit(0)), 4).alias("event_at_dense_rank_share_of_event_at_seq"),
    )
)

result = (
    ranked
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_at_plus_7d",
        "distinct_event_type_in_event_type_len_ticket_id",
        "next_event_type_len",
        "event_at_dense_rank",
        "event_type_len_sum_4",
        "prev_event_type_len",
        "event_at_dense_rank_sum_6_prior",
        "running_event_type_len_vs_event_at_seq_avg",
        "event_at_dense_rank_share_of_event_at_seq",
        F.to_date(F.col("event_at_plus_7d")).alias("event_at_plus_7d_date"),
    )
    .orderBy(F.col("event_at_dense_rank_share_of_event_at_seq").desc(), "id")
    .limit(20)
)
