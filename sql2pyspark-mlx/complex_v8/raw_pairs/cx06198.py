from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_prior_csat_3d = Window.orderBy("prior_csat_3d", "id")
w_prior_rows_3d = Window.partitionBy("prior_rows_3d")
w_distinct_agent_id_in_prior_r = Window.partitionBy("distinct_agent_id_in_prior_rows_3d", "agent_id")
w_all_by_distinct_agent_id_in = Window.orderBy(F.col("distinct_agent_id_in_prior_rows_3d").desc_nulls_first(), "id")
w_prior_rows_3d_by_prior_rows = Window.partitionBy("prior_rows_3d").orderBy("prior_rows_3d", "id")
w_prior_rows_3d_by_prior_rows_2 = Window.partitionBy("prior_rows_3d").orderBy("prior_rows_3d", "id").rowsBetween(-1, Window.currentRow)
w_prior_rows_3d_by_prior_rows_3 = Window.partitionBy("prior_rows_3d").orderBy("prior_rows_3d")
w_all_by_distinct_agent_id_in_2 = Window.orderBy(F.col("distinct_agent_id_in_prior_rows_3d").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

joined_ranges = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.csat").alias("prior_csat_3d"),
    )
)

ranked = (
    joined_ranges
    .select(
        "*",
        F.row_number().over(w_all_by_prior_csat_3d).alias("prior_csat_3d_seq"),
        F.size(F.collect_set("agent_id").over(w_prior_rows_3d)).alias("distinct_agent_id_in_prior_rows_3d"),
        F.round(F.avg("prior_rows_3d").over(w_prior_rows_3d), 2).alias("prior_rows_3d_avg_prior_rows_3d"),
    )
)

sequenced = (
    ranked
    .select(
        "*",
        F.ntile(10).over(w_all_by_distinct_agent_id_in).alias("distinct_agent_id_in_prior_rows_3d_decile"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_prior_rows_3d_by_prior_rows))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.sum("prior_csat_3d_seq").over(w_prior_rows_3d_by_prior_rows_2).alias("prior_csat_3d_seq_sum_2"),
        F.round(F.avg("prior_csat_3d_seq").over(w_distinct_agent_id_in_prior_r), 2).alias("distinct_agent_id_in_prior_rows_3d_agent_id_avg_"),
        F.dense_rank().over(w_prior_rows_3d_by_prior_rows_3).alias("prior_rows_3d_dense_rank"),
        F.last("prior_rows_3d").over(w_all_by_distinct_agent_id_in_2).alias("last_prior_rows_3d"),
    )
)

filtered = (
    sequenced.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_at") >= F.lit("2025-01-13 14:00:00").cast("timestamp")) & (F.col("t.event_type") != "assigned"), "left_anti")
)

result = (
    filtered
    .select(
        "id",
        "opened_at",
        "prior_rows_3d",
        "prior_csat_3d",
        "prior_csat_3d_seq",
        "distinct_agent_id_in_prior_rows_3d",
        "prior_rows_3d_avg_prior_rows_3d",
        "distinct_agent_id_in_prior_rows_3d_decile",
        "prior_csat_3d_seq_sum_2",
        "distinct_agent_id_in_prior_rows_3d_agent_id_avg_",
        "prior_rows_3d_dense_rank",
        "last_prior_rows_3d",
        F.when(F.col("prior_csat_3d") >= 19, "HIGH").when(F.col("prior_csat_3d") >= 6, "MEDIUM").otherwise("LOW").alias("prior_csat_3d_tier"),
    )
)
