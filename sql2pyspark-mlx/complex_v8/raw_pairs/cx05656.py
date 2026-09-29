from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_or_unknown_by_eve = Window.partitionBy("event_type_or_unknown").orderBy("event_at")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_all_by_event_at_ties = Window.orderBy("event_at")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_ties_2 = Window.orderBy(F.col("event_at").desc())
w_all_by_event_at_2 = Window.orderBy(F.col("event_at").desc(), "id")
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")
w_next_event_at_month = Window.partitionBy(F.date_trunc("month", "next_event_at"))
w_event_at_rank_event_type_or = Window.partitionBy("event_at_rank", "event_type_or_unknown").orderBy("event_at_percent_rank", "id")
w_actor_next2_event_at_month_b = Window.partitionBy("actor", F.month("next2_event_at")).orderBy("first_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_or_unknown_by_eve_2 = Window.partitionBy("event_type_or_unknown").orderBy(F.col("event_at_percent_rank_2").desc(), "id").rowsBetween(-1, -1)
w_event_type_or_unknown_by_pct = Window.partitionBy("event_type_or_unknown").orderBy(F.col("pct_actor_not_in_customer_system").desc(), "prev_event_at_day", "event_type_or_unknown")
w_cnt_event_type_or_unknown = Window.partitionBy("cnt", "event_type_or_unknown")
w_event_type_or_unknown_by_pct_2 = Window.partitionBy("event_type_or_unknown").orderBy("pct_actor_not_in_customer_system", "prev_event_at_day", "event_type_or_unknown")

valid_events = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-02-24").cast("date")) & (F.col("actor") == "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("actor"))).alias("actor_clean"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

with_trends = (
    valid_events
    .select(
        "id",
        "event_type",
        "actor",
        "actor_clean",
        "event_type_or_unknown",
        F.round(F.percent_rank().over(w_event_type_or_unknown_by_eve), 3).alias("event_at_percent_rank"),
        F.lag("event_at").over(w_all_by_event_at).alias("prev_event_at"),
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.row_number().over(w_all_by_event_at).alias("rn"),
        F.round(F.percent_rank().over(w_all_by_event_at_ties_2), 3).alias("event_at_percent_rank_2"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.lag("event_at", 2).over(w_all_by_event_at_2).alias("prev2_event_at"),
        F.round(F.percent_rank().over(w_all_by_event_at_ties), 3).alias("event_at_percent_rank_3"),
        F.lead("event_at", 2).over(w_all_by_event_at_2).alias("next2_event_at"),
        F.dense_rank().over(w_all_by_event_at).alias("event_at_dense_rank"),
        F.lead("event_at").over(w_event_type_by_event_at).alias("next_event_at"),
    )
)

top_rows = with_trends.filter(F.col("event_at_rank") == 1)

sequenced = (
    top_rows
    .select(
        "id",
        "event_type",
        "actor",
        "event_type_or_unknown",
        "prev_event_at",
        "first_event_at",
        "rn",
        "event_at_percent_rank_2",
        "prev2_event_at",
        "event_at_percent_rank_3",
        "event_at_dense_rank",
        "next_event_at",
        F.size(F.collect_set("event_type_or_unknown").over(w_next_event_at_month)).alias("distinct_event_type_or_unknown_in_next_event_at_"),
        F.rank().over(w_event_at_rank_event_type_or).alias("event_at_percent_rank_rank"),
        F.last("event_at_cume_dist", ignorenulls=True).over(w_actor_next2_event_at_month_b).alias("last_known_event_at_cume_dist"),
        F.round(F.col("event_at_percent_rank") / F.nullif(F.sum("event_at_percent_rank").over(w_next_event_at_month), F.lit(0)), 4).alias("event_at_percent_rank_share_of_next_event_at_mon"),
        F.round(F.avg("rn").over(w_event_type_or_unknown_by_eve_2), 2).alias("rn_avg_1_prior"),
        F.count(F.lit(1)).over(w_next_event_at_month).alias("rows_in_next_event_at_month"),
    )
)

totals = (
    sequenced
    .groupBy(
        F.to_date("prev_event_at").alias("prev_event_at_day"),
        "event_type_or_unknown",
    )
    .agg(
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.round(100 * F.count(F.when(~F.col("actor").isin("customer", "system"), 1)) / F.count("*"), 1).alias("pct_actor_not_in_customer_system"),
        F.count("*").alias("cnt"),
    )
)

ranked = (
    totals
    .select(
        "prev_event_at_day",
        "event_type_or_unknown",
        "actor_list",
        "cnt",
        F.lead("cnt").over(w_event_type_or_unknown_by_pct).alias("next_cnt"),
        F.count(F.lit(1)).over(w_cnt_event_type_or_unknown).alias("rows_in_cnt_event_type_or_unknown"),
        F.lead("cnt", 2).over(w_event_type_or_unknown_by_pct).alias("next2_cnt"),
        F.dense_rank().over(w_event_type_or_unknown_by_pct_2).alias("pct_actor_not_in_customer_system_dense_rank"),
    )
)

result = (
    ranked
    .select(
        "prev_event_at_day",
        "event_type_or_unknown",
        "actor_list",
        "cnt",
        "rows_in_cnt_event_type_or_unknown",
        "next2_cnt",
        "pct_actor_not_in_customer_system_dense_rank",
        F.round(F.col("next_cnt") / F.nullif(F.col("next2_cnt"), F.lit(0)), 3).alias("next_cnt_per_next2_cnt"),
        F.substring(F.trim(F.col("event_type_or_unknown")), 1, 3).alias("event_type_or_unknown_prefix"),
    )
    .orderBy(F.col("pct_actor_not_in_customer_system_dense_rank").asc())
)
