from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id = Window.partitionBy("ticket_id")
w_all_by_event_at_moving_2 = Window.orderBy("event_at", "id").rowsBetween(-2, Window.currentRow)
w_all_by_event_at = Window.orderBy("event_at", "id")
w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last())
w_actor_by_event_at_prior_1 = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_event_at_quarter_by_event_at = Window.partitionBy("event_at_quarter").orderBy(F.col("event_at").desc(), "id")
w_actor_prev3_event_at_quarter = Window.partitionBy("actor", "prev3_event_at_quarter_filled")
w_all_by_event_at_quarter_avg = Window.orderBy("event_at_quarter_avg_1_prior", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_event_at_seq_by_pr = Window.partitionBy("ticket_id", "event_at_seq").orderBy("prev3_event_at_quarter", "id")
w_all_by_rows_in_ticket_id_ful = Window.orderBy(F.col("rows_in_ticket_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_rows_in_ticket_id_run = Window.orderBy(F.col("rows_in_ticket_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 45) & (F.col("event_type") != "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.quarter(F.col("event_at")).alias("event_at_quarter"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

windowed = (
    scoped
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_type_slug",
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at"),
        F.round(F.avg("event_at_quarter").over(w_ticket_id), 2).alias("ticket_id_avg_event_at_quarter"),
        F.round(F.col("event_at_quarter") - F.avg("event_at_quarter").over(w_ticket_id), 2).alias("event_at_quarter_vs_ticket_id_avg"),
        F.max("event_at_quarter").over(w_all_by_event_at_moving_2).alias("event_at_quarter_max_3"),
        F.lag("event_at_quarter", 3).over(w_all_by_event_at).alias("prev3_event_at_quarter"),
        F.count(F.lit(1)).over(w_ticket_id).alias("rows_in_ticket_id"),
        F.ntile(4).over(w_all_by_event_at).alias("event_at_quartile"),
        F.round(F.cume_dist().over(w_actor_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.round(F.avg("event_at_quarter").over(w_actor_by_event_at_prior_1), 2).alias("event_at_quarter_avg_1_prior"),
        F.size(F.collect_set("actor").over(w_ticket_id)).alias("distinct_actor_in_ticket_id"),
        F.row_number().over(w_event_at_quarter_by_event_at).alias("event_at_seq"),
    )
)

derived = (
    windowed
    .withColumn("prev3_event_at_quarter_filled", F.coalesce(F.col("prev3_event_at_quarter"), F.lit(0)))
)

with_trends = (
    derived
    .select(
        "*",
        F.round(F.col("distinct_actor_in_ticket_id") / F.nullif(F.sum("distinct_actor_in_ticket_id").over(w_actor_prev3_event_at_quarter), F.lit(0)), 4).alias("distinct_actor_in_ticket_id_share_of_actor_prev3"),
        F.last("last_event_at").over(w_all_by_event_at_quarter_avg).alias("last_last_event_at"),
        F.round(F.percent_rank().over(w_ticket_id_event_at_seq_by_pr), 3).alias("prev3_event_at_quarter_percent_rank"),
        F.round(F.col("event_at_seq") / F.nullif(F.sum("event_at_seq").over(w_actor_prev3_event_at_quarter), F.lit(0)), 4).alias("event_at_seq_share_of_actor_prev3_event_at_quart"),
        F.nth_value("event_at", 2).over(w_all_by_rows_in_ticket_id_ful).alias("second_event_at"),
        F.count(F.lit(1)).over(w_all_by_rows_in_ticket_id_run).alias("running_count"),
    )
)

result = (
    with_trends
    .filter(F.col("prev3_event_at_quarter") >= 2)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "last_event_at",
        "ticket_id_avg_event_at_quarter",
        "event_at_quarter_vs_ticket_id_avg",
        "event_at_quarter_max_3",
        "prev3_event_at_quarter",
        "rows_in_ticket_id",
        "event_at_quartile",
        "event_at_cume_dist",
        "event_at_quarter_avg_1_prior",
        "distinct_actor_in_ticket_id",
        "prev3_event_at_quarter_filled",
        "distinct_actor_in_ticket_id_share_of_actor_prev3",
        "last_last_event_at",
        "prev3_event_at_quarter_percent_rank",
        "event_at_seq_share_of_actor_prev3_event_at_quart",
        "second_event_at",
        "running_count",
        F.when(F.col("rows_in_ticket_id") >= 16, "large").when(F.col("rows_in_ticket_id") >= 8, "medium").otherwise("small").alias("rows_in_ticket_id_tier"),
        F.when(F.col("actor").isin("customer", "agent"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
    )
    .orderBy(F.col("last_event_at").asc(), "event_at_quartile", "id")
    .limit(5)
)
