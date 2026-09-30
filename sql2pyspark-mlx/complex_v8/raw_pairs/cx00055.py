from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_ticket_id_by_actor_len = Window.partitionBy("actor", "ticket_id").orderBy("actor_len", "id")
w_actor_len = Window.partitionBy("actor_len")
w_ticket_id_by_distinct_event = Window.partitionBy("ticket_id").orderBy("distinct_event_type_in_actor_len", "id")
w_all_by_distinct_event_type_i = Window.orderBy("distinct_event_type_in_actor_len")
w_event_at_month = Window.partitionBy(F.date_trunc("month", "event_at"))
w_all_by_distinct_event_type_i_2 = Window.orderBy("distinct_event_type_in_actor_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_event_type_in_actor = Window.partitionBy("distinct_event_type_in_actor_len").orderBy("distinct_event_type_in_event_at_month", "id").rowsBetween(-3, Window.currentRow)
w_event_at_month_seq_no = Window.partitionBy(F.month("event_at"), "seq_no")
w_actor_distinct_event_type_in = Window.partitionBy("actor", "distinct_event_type_in_actor_len").orderBy(F.col("distinct_event_type_in_actor_len").desc())
w_actor_distinct_event_type_in_2 = Window.partitionBy("actor", "distinct_event_type_in_actor_len").orderBy(F.col("distinct_event_type_in_actor_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_event_type_in_actor_2 = Window.partitionBy("distinct_event_type_in_actor_len").orderBy("distinct_event_type_in_event_at_month", "id").rowsBetween(-1, Window.currentRow)

scoped_events = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-03-08 10:00:00").cast("timestamp")) & (F.col("actor") == "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

ranked = (
    scoped_events
    .select(
        "*",
        F.lead("actor_len", 1, 0).over(w_actor_ticket_id_by_actor_len).alias("next_actor_len"),
        F.size(F.collect_set("event_type").over(w_actor_len)).alias("distinct_event_type_in_actor_len"),
        F.count(F.lit(1)).over(w_actor_len).alias("rows_in_actor_len"),
        F.round(F.col("actor_len") / F.nullif(F.sum("actor_len").over(w_actor_len), F.lit(0)), 4).alias("actor_len_share_of_actor_len"),
    )
)

sequenced = (
    ranked
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "actor_len",
        "days_since_event_at",
        "distinct_event_type_in_actor_len",
        "actor_len_share_of_actor_len",
        F.row_number().over(w_ticket_id_by_distinct_event).alias("seq_no"),
        F.round(F.cume_dist().over(w_all_by_distinct_event_type_i), 3).alias("distinct_event_type_in_actor_len_cume_dist"),
        F.size(F.collect_set("event_type").over(w_event_at_month)).alias("distinct_event_type_in_event_at_month"),
        F.last("rows_in_actor_len", ignorenulls=True).over(w_all_by_distinct_event_type_i_2).alias("last_known_rows_in_actor_len"),
        F.last("actor_len", ignorenulls=True).over(w_all_by_distinct_event_type_i_2).alias("last_known_actor_len"),
    )
)

metrics = (
    sequenced
    .select(
        "*",
        F.round(F.avg("seq_no").over(w_distinct_event_type_in_actor), 2).alias("seq_no_avg_4"),
        F.size(F.collect_set("actor").over(w_event_at_month_seq_no)).alias("distinct_actor_in_event_at_month_seq_no"),
        F.max("seq_no").over(w_event_at_month_seq_no).alias("event_at_month_seq_no_max_seq_no"),
        F.dense_rank().over(w_actor_distinct_event_type_in).alias("distinct_event_type_in_actor_len_dense_rank"),
        F.nth_value("actor_len_share_of_actor_len", 2).over(w_actor_distinct_event_type_in_2).alias("second_actor_len_share_of_actor_len"),
        F.max("last_known_rows_in_actor_len").over(w_distinct_event_type_in_actor_2).alias("last_known_rows_in_actor_len_max_2"),
    )
)

result = (
    metrics
    .rollup("distinct_event_type_in_actor_len_dense_rank", "distinct_event_type_in_actor_len")
    .agg(
        F.grouping_id().alias("gid"),
        F.min("distinct_event_type_in_actor_len_dense_rank").alias("min_distinct_event_type_in_actor_len_dense_rank"),
        F.count(F.when(~F.col("event_type").isin("created", "resolved"), 1)).alias("n_event_type_not_in_created_re"),
        F.percentile_approx("distinct_event_type_in_actor_len", 0.5).alias("median_distinct_event_type_in_actor_len"),
    )
    .orderBy(
        F.col("distinct_event_type_in_actor_len_dense_rank").asc_nulls_last(),
        F.col("distinct_event_type_in_actor_len").asc_nulls_last(),
    )
)
