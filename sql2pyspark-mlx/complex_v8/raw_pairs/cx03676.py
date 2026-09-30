from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_to_date_actor_len_b = Window.partitionBy(F.to_date("event_at"), "actor_len").orderBy(F.col("actor_len").desc(), "id")
w_ticket_id = Window.partitionBy("ticket_id")
w_event_at_to_date_actor_len_b_2 = Window.partitionBy(F.to_date("event_at"), "actor_len").orderBy(F.col("actor_len").desc(), "id").rowsBetween(-6, Window.currentRow)
w_actor_event_at_year_by_actor = Window.partitionBy("actor", F.year("event_at")).orderBy(F.col("actor_len").desc(), "id")
w_all_by_actor_len = Window.orderBy(F.col("actor_len").desc(), "id")
w_actor_len_ticket_id_by_actor = Window.partitionBy("actor_len", "ticket_id").orderBy(F.col("actor_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy(F.col("event_at").desc_nulls_first(), "id")
w_ticket_id_actor_len = Window.partitionBy("ticket_id", "actor_len")
w_actor_len_ticket_id_by_actor_2 = Window.partitionBy("actor_len", "ticket_id").orderBy(F.col("actor_len").desc())
w_all_by_actor_len_full = Window.orderBy(F.col("actor_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_event_type = Window.partitionBy("actor", "event_type")
w_event_type_actor_by_actor_le = Window.partitionBy("event_type", "actor").orderBy(F.col("actor_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_ticket_events
    .filter((F.col("ticket_id") <= 53) & (F.col("actor").isin("system", "customer")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
    )
)

sequenced = (
    scoped
    .select(
        "id",
        "actor",
        F.lead("event_at", 3).over(w_event_at_to_date_actor_len_b).alias("next3_event_at"),
        F.sum("actor_len").over(w_ticket_id).alias("ticket_id_sum_actor_len"),
        F.round(F.col("actor_len") / F.nullif(F.sum("actor_len").over(w_ticket_id), F.lit(0)), 4).alias("actor_len_share_of_ticket_id"),
        F.sum("actor_len").over(w_event_at_to_date_actor_len_b_2).alias("actor_len_sum_7"),
        F.dense_rank().over(w_actor_event_at_year_by_actor).alias("actor_len_dense_rank"),
        F.rank().over(w_all_by_actor_len).alias("actor_len_rank"),
        F.nth_value("actor_len", 2).over(w_actor_len_ticket_id_by_actor).alias("second_actor_len"),
        F.rank().over(w_all_by_event_at).alias("event_at_rank"),
        F.round(F.avg("actor_len").over(w_ticket_id_actor_len), 2).alias("ticket_id_actor_len_avg_actor_len"),
        F.round(F.percent_rank().over(w_actor_len_ticket_id_by_actor_2), 3).alias("actor_len_percent_rank"),
        F.first("actor_len").over(w_all_by_actor_len_full).alias("first_actor_len"),
        F.max("actor_len").over(w_actor_event_type).alias("actor_event_type_max_actor_len"),
        F.nth_value("event_at", 2).over(w_event_type_actor_by_actor_le).alias("second_event_at"),
        F.size(F.collect_set("ticket_id").over(w_actor_event_type)).alias("distinct_ticket_id_in_actor_event_type"),
    )
)

grouped = (
    sequenced
    .groupBy("distinct_ticket_id_in_actor_event_type", "event_at_rank")
    .agg(
        F.round(F.sum("actor_len_dense_rank") / F.nullif(F.sum("first_actor_len"), F.lit(0)), 3).alias("actor_len_dense_rank_to_first_actor_len_ratio"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.min("next3_event_at").alias("first_next3_event_at"),
    )
)

result = (
    grouped
    .select(
        "distinct_ticket_id_in_actor_event_type",
        "event_at_rank",
        "actor_len_dense_rank_to_first_actor_len_ratio",
        "distinct_actors",
        "first_next3_event_at",
    )
    .orderBy(
        F.col("distinct_ticket_id_in_actor_event_type").desc_nulls_first(),
        "event_at_rank",
    )
)
