from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at_prior = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_event_type_len_event_type = Window.partitionBy("event_type_len", "event_type")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc_nulls_first(), "id")
w_ticket_id_by_event_at_runnin = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_len_by_event_type = Window.partitionBy("event_type_len").orderBy(F.col("event_type_len").desc(), "id")
w_all_by_running_event_type_le = Window.orderBy("running_event_type_len_distance", "id")
w_event_type_group_event_type = Window.partitionBy("event_type_group", "event_type_len").orderBy("event_type_len_max_2_prior", "id").rowsBetween(-1, Window.currentRow)
w_event_type_group_event_type_2 = Window.partitionBy("event_type_group", "event_type_len").orderBy("event_type_len_max_2_prior", "id")
w_all_by_event_type_len_share = Window.orderBy(F.col("event_type_len_share_of_event_type_len_event_typ").desc(), "id")
w_all_by_running_event_type_le_2 = Window.orderBy("running_event_type_len_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_group_event_type_3 = Window.partitionBy("event_type_group", "event_type_len").orderBy("event_type_len_max_2_prior")
w_all_by_event_type_len_share_2 = Window.orderBy(F.col("event_type_len_share_of_event_type_len_event_typ").desc())

base_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

sequenced = (
    base_events
    .select(
        "*",
        F.max("event_type_len").over(w_ticket_id_by_event_at_prior).alias("event_type_len_max_2_prior"),
        F.round(F.col("event_type_len") / F.nullif(F.sum("event_type_len").over(w_event_type_len_event_type), F.lit(0)), 4).alias("event_type_len_share_of_event_type_len_event_typ"),
        F.dense_rank().over(w_ticket_id_by_event_at).alias("event_at_dense_rank"),
        F.round(F.cume_dist().over(w_ticket_id_by_event_at), 3).alias("event_at_cume_dist"),
        F.sum("event_type_len").over(w_ticket_id_by_event_at_runnin).alias("running_event_type_len"),
        F.ntile(4).over(w_event_type_len_by_event_type).alias("event_type_len_quartile"),
        F.rank().over(w_event_type_len_by_event_type).alias("event_type_len_rank"),
    )
)

derived = (
    sequenced
    .select(
        "*",
        F.round(F.col("event_type_len_quartile") / F.nullif(F.col("event_type_len_rank"), F.lit(0)), 3).alias("event_type_len_quartile_per_event_type_len_rank"),
        F.when(F.col("event_type").isin("assigned", "replied"), "group_a").when(F.col("event_type") == "escalated", "group_b").otherwise("other").alias("event_type_group"),
        F.abs(F.col("running_event_type_len") - 85).alias("running_event_type_len_distance"),
    )
)

with_trends = (
    derived
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_prefix",
        "event_type_len",
        "event_type_len_max_2_prior",
        "event_at_cume_dist",
        "running_event_type_len",
        "event_type_len_quartile",
        "event_type_group",
        "running_event_type_len_distance",
        F.dense_rank().over(w_all_by_running_event_type_le).alias("running_event_type_len_distance_dense_rank"),
        F.round(F.avg("event_type_len").over(w_event_type_group_event_type), 2).alias("event_type_len_avg_2"),
        F.rank().over(w_event_type_group_event_type_2).alias("event_type_len_max_2_prior_rank"),
        F.ntile(5).over(w_all_by_event_type_len_share).alias("event_type_len_share_of_event_type_len_event_typ_2"),
        F.first("event_at").over(w_all_by_running_event_type_le_2).alias("first_event_at"),
        F.round(F.percent_rank().over(w_event_type_group_event_type_3), 3).alias("event_type_len_max_2_prior_percent_rank"),
        F.row_number().over(w_event_type_group_event_type_2).alias("rn"),
        F.dense_rank().over(w_all_by_event_type_len_share_2).alias("event_type_len_share_of_event_type_len_event_typ_3"),
    )
)

result = (
    with_trends
    .filter(F.col("event_type_len_max_2_prior_rank") >= 2)
    .select(
        "id",
        "event_at",
        "actor_prefix",
        "event_type_len",
        "event_type_len_max_2_prior",
        "event_at_cume_dist",
        "event_type_len_quartile",
        "event_type_group",
        "running_event_type_len_distance_dense_rank",
        "event_type_len_avg_2",
        "event_type_len_max_2_prior_rank",
        "event_type_len_share_of_event_type_len_event_typ_2",
        "first_event_at",
        "event_type_len_max_2_prior_percent_rank",
        "rn",
        F.coalesce(F.col("rn"), F.lit(0)).alias("rn_filled"),
        F.substring(F.trim(F.col("actor_prefix")), 1, 4).alias("actor_prefix_prefix"),
    )
    .orderBy(F.col("event_type_len_max_2_prior_rank").desc(), "id")
    .limit(20)
)
