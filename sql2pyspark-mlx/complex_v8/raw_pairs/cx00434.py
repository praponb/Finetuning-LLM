from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_csat = Window.partitionBy("agent_id", "csat")
w_agent_id_by_opened_at_ties = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc_nulls_first())
w_priority_csat = Window.partitionBy("priority", "csat")
w_all_by_requester_id_per_csat = Window.orderBy("requester_id_per_csat", "id").rowsBetween(-1, Window.currentRow)
w_csat_opened_at_month_by_clos = Window.partitionBy("csat", F.month("opened_at")).orderBy("closed_at", "id").rowsBetween(-6, -1)
w_priority_by_csat = Window.partitionBy("priority").orderBy("csat", "id")
w_csat_opened_at_month_by_clos_2 = Window.partitionBy("csat", F.month("opened_at")).orderBy("closed_at")
w_all_by_requester_id_per_csat_2 = Window.orderBy("requester_id_per_csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_requester_id_per_csat_3 = Window.orderBy("requester_id_per_csat", "id")
w_csat_opened_at_month_by_clos_3 = Window.partitionBy("csat", F.month("opened_at")).orderBy("closed_at", "id").rowsBetween(-2, -1)
w_csat = Window.partitionBy("csat")
w_agent_id_by_opened_at = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc(), "id")
w_distinct_priority_in_agent_i = Window.partitionBy("distinct_priority_in_agent_id_csat").orderBy(F.col("opened_at_dense_rank").desc(), "id")

prepared = (
    df_tickets
    .filter((F.to_date(F.col("closed_at")) < F.lit("2025-02-26").cast("date")) & (F.col("opened_at") < F.lit("2025-02-18 11:00:00").cast("timestamp")))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
    )
)

sequenced = (
    prepared
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "requester_id_per_csat",
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_agent_id_csat), F.lit(0)), 4).alias("csat_share_of_agent_id_csat"),
        F.dense_rank().over(w_agent_id_by_opened_at_ties).alias("opened_at_dense_rank"),
        F.count(F.lit(1)).over(w_agent_id_csat).alias("rows_in_agent_id_csat"),
        F.round(F.col("requester_id_per_csat") / F.nullif(F.sum("requester_id_per_csat").over(w_priority_csat), F.lit(0)), 4).alias("requester_id_per_csat_share_of_priority_csat"),
        F.size(F.collect_set("priority").over(w_agent_id_csat)).alias("distinct_priority_in_agent_id_csat"),
        F.round(F.avg("requester_id_per_csat").over(w_all_by_requester_id_per_csat), 2).alias("requester_id_per_csat_avg_2"),
        F.size(F.collect_set("agent_id").over(w_priority_csat)).alias("distinct_agent_id_in_priority_csat"),
        F.round(F.avg("requester_id_per_csat").over(w_csat_opened_at_month_by_clos), 2).alias("requester_id_per_csat_avg_6_prior"),
        F.dense_rank().over(w_priority_by_csat).alias("csat_dense_rank"),
        F.round(F.cume_dist().over(w_csat_opened_at_month_by_clos_2), 3).alias("closed_at_cume_dist"),
        F.round(F.col("requester_id_per_csat") - F.avg("requester_id_per_csat").over(w_agent_id_csat), 2).alias("requester_id_per_csat_vs_agent_id_csat_avg"),
        F.first("opened_at", ignorenulls=True).over(w_all_by_requester_id_per_csat_2).alias("first_known_opened_at"),
        F.row_number().over(w_all_by_requester_id_per_csat_3).alias("requester_id_per_csat_seq"),
        F.max("requester_id_per_csat").over(w_csat_opened_at_month_by_clos_3).alias("requester_id_per_csat_max_2_prior"),
    )
)

leaders = (
    sequenced
    .filter((F.col("requester_id_per_csat_seq") == 1) & (F.col("csat") < 4))
    .drop("requester_id_per_csat_seq")
)

windowed = (
    leaders
    .select(
        "*",
        F.round(F.col("csat") - F.avg("csat").over(w_csat), 2).alias("csat_vs_csat_avg"),
        F.lead("csat_share_of_agent_id_csat").over(w_agent_id_by_opened_at).alias("next_csat_share_of_agent_id_csat"),
        F.row_number().over(w_distinct_priority_in_agent_i).alias("opened_at_dense_rank_seq"),
        F.count(F.lit(1)).over(w_csat).alias("rows_in_csat"),
        F.size(F.collect_set("agent_id").over(w_csat)).alias("distinct_agent_id_in_csat"),
    )
)

result = (
    windowed
    .select(
        "id",
        "agent_id",
        "priority",
        "csat",
        "requester_id_per_csat",
        "csat_share_of_agent_id_csat",
        "opened_at_dense_rank",
        "rows_in_agent_id_csat",
        "requester_id_per_csat_avg_2",
        "requester_id_per_csat_avg_6_prior",
        "csat_dense_rank",
        "closed_at_cume_dist",
        "requester_id_per_csat_vs_agent_id_csat_avg",
        "first_known_opened_at",
        "requester_id_per_csat_max_2_prior",
        "next_csat_share_of_agent_id_csat",
        "opened_at_dense_rank_seq",
        "rows_in_csat",
        "distinct_agent_id_in_csat",
        F.initcap(F.trim(F.col("priority"))).alias("priority_clean"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
    .orderBy(
        F.col("rows_in_agent_id_csat").asc(),
        F.col("priority_or_unknown").desc(),
    )
)
