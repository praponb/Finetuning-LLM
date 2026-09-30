from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_opened_at = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_priority_by_opened_at = Window.partitionBy("priority").orderBy("opened_at", "id")
w_priority_by_opened_at_full = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_opened_at_ties = Window.partitionBy("agent_id").orderBy("opened_at")
w_priority_agent_id_by_closed = Window.partitionBy("priority", "agent_id").orderBy(F.col("closed_at").desc(), "id")
w_priority_agent_id_by_closed_2 = Window.partitionBy("priority", "agent_id").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_priority_by_opened = Window.partitionBy("agent_id", "priority").orderBy(F.col("opened_at").asc_nulls_last(), "id")
w_first_closed_at_to_date_clos = Window.partitionBy(F.to_date("first_closed_at"), F.year("closed_at"))
w_opened_at_quintile_rn_by_fir = Window.partitionBy("opened_at_quintile", "rn").orderBy("first_closed_at")
w_closed_at_rank_second_closed = Window.partitionBy("closed_at_rank", F.month("second_closed_at")).orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_quintile_by_first = Window.partitionBy("opened_at_quintile").orderBy("first_closed_at", "id").rowsBetween(-3, -1)
w_agent_id_closed_at_rank_by_o = Window.partitionBy("agent_id", "closed_at_rank").orderBy(F.col("opened_at_cume_dist").desc(), "id").rowsBetween(-3, Window.currentRow)
w_second_closed_at_to_date = Window.partitionBy(F.to_date("second_closed_at"))
w_closed_at_rank_second_closed_2 = Window.partitionBy("closed_at_rank", F.month("second_closed_at")).orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_quintile_rn_by_fir_2 = Window.partitionBy("opened_at_quintile", "rn").orderBy("first_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_tickets = (
    df_tickets
    .filter(F.col("opened_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "tags",
    )
)

metrics = (
    scoped_tickets
    .select(
        "*",
        F.round(F.percent_rank().over(w_agent_id_by_opened_at), 3).alias("opened_at_percent_rank"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_priority_by_opened_at))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.ntile(5).over(w_priority_by_opened_at).alias("opened_at_quintile"),
        F.first("closed_at").over(w_priority_by_opened_at_full).alias("first_closed_at"),
        F.round(F.percent_rank().over(w_agent_id_by_opened_at_ties), 3).alias("opened_at_percent_rank_2"),
        F.row_number().over(w_priority_agent_id_by_closed).alias("rn"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_priority_by_opened_at))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.nth_value("closed_at", 2).over(w_priority_agent_id_by_closed_2).alias("second_closed_at"),
        F.round(F.cume_dist().over(w_agent_id_priority_by_opened), 3).alias("opened_at_cume_dist"),
        F.rank().over(w_priority_agent_id_by_closed).alias("closed_at_rank"),
    )
)

with_trends = (
    metrics
    .select(
        "*",
        F.round(F.col("minutes_since_prev_closed_at") - F.avg("minutes_since_prev_closed_at").over(w_first_closed_at_to_date_clos), 2).alias("minutes_since_prev_closed_at_vs_first_closed_at_"),
        F.dense_rank().over(w_opened_at_quintile_rn_by_fir).alias("first_closed_at_dense_rank"),
        F.first("first_closed_at", ignorenulls=True).over(w_closed_at_rank_second_closed).alias("first_known_first_closed_at"),
        F.round(F.avg("opened_at_percent_rank").over(w_opened_at_quintile_by_first), 2).alias("opened_at_percent_rank_avg_3_prior"),
        F.round(F.avg("opened_at_percent_rank").over(w_agent_id_closed_at_rank_by_o), 2).alias("opened_at_percent_rank_avg_4"),
        F.round(F.col("minutes_since_prev_closed_at") - F.avg("minutes_since_prev_closed_at").over(w_second_closed_at_to_date), 2).alias("minutes_since_prev_closed_at_vs_second_closed_at"),
        F.nth_value("minutes_since_prev_opened_at", 2).over(w_closed_at_rank_second_closed_2).alias("second_minutes_since_prev_opened_at"),
        F.count(F.lit(1)).over(w_opened_at_quintile_rn_by_fir_2).alias("running_count"),
    )
)

result = (
    with_trends
    .filter(F.col("running_count") >= 4)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "opened_at_percent_rank",
        "minutes_since_prev_closed_at",
        "opened_at_quintile",
        "first_closed_at",
        "opened_at_percent_rank_2",
        "rn",
        "minutes_since_prev_opened_at",
        "closed_at_rank",
        "minutes_since_prev_closed_at_vs_first_closed_at_",
        "first_known_first_closed_at",
        "opened_at_percent_rank_avg_3_prior",
        "opened_at_percent_rank_avg_4",
        "minutes_since_prev_closed_at_vs_second_closed_at",
        "second_minutes_since_prev_opened_at",
        (F.col("opened_at_percent_rank_2") - F.col("rn")).alias("opened_at_percent_rank_2_minus_rn"),
    )
    .orderBy(
        "opened_at_percent_rank_avg_3_prior",
        F.col("opened_at").desc_nulls_first(),
    )
)
