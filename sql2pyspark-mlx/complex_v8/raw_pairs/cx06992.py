from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat = Window.partitionBy("csat")
w_agent_id = Window.partitionBy("agent_id")
w_channel_opened_at_year_by_re = Window.partitionBy("channel", F.year("opened_at")).orderBy(F.col("requester_id").desc(), "id")
w_agent_id_by_requester_id = Window.partitionBy("agent_id").orderBy(F.col("requester_id").asc_nulls_last(), "id")
w_priority_by_closed_at_full = Window.partitionBy("priority").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_closed_at_to_date = Window.partitionBy(F.to_date("closed_at"))
w_all_by_csat = Window.orderBy("csat", "id")
w_csat_opened_at_month_by_requ = Window.partitionBy("csat", F.date_trunc("month", "opened_at")).orderBy("requester_id", "id")
w_agent_id_by_requester_id_run = Window.partitionBy("agent_id").orderBy(F.col("requester_id").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_priority_by_request = Window.partitionBy("agent_id", "priority").orderBy("requester_id", "id")

scoped = (
    df_tickets
    .filter(F.col("channel").isin("web", "email"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

sequenced = (
    scoped
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.size(F.collect_set("agent_id").over(w_csat)).alias("distinct_agent_id_in_csat"),
        F.min("csat").over(w_agent_id).alias("agent_id_min_csat"),
        F.row_number().over(w_channel_opened_at_year_by_re).alias("rn"),
        F.round(F.cume_dist().over(w_agent_id_by_requester_id), 3).alias("requester_id_cume_dist"),
        F.nth_value("closed_at", 2).over(w_priority_by_closed_at_full).alias("second_closed_at"),
        F.sum("requester_id").over(w_csat).alias("csat_sum_requester_id"),
        F.size(F.collect_set("channel").over(w_closed_at_to_date)).alias("distinct_channel_in_closed_at_to_date"),
        F.ntile(3).over(w_all_by_csat).alias("csat_tercile"),
        F.ntile(4).over(w_csat_opened_at_month_by_requ).alias("requester_id_quartile"),
        F.count(F.lit(1)).over(w_closed_at_to_date).alias("rows_in_closed_at_to_date"),
        F.last("csat", ignorenulls=True).over(w_agent_id_by_requester_id_run).alias("last_known_csat"),
        F.count(F.lit(1)).over(w_csat).alias("rows_in_csat"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_agent_id_priority_by_request))) / 60, 1).alias("minutes_since_prev_opened_at"),
    )
)

leaders = sequenced.filter(F.col("rn") == 1)

result = (
    leaders
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "distinct_agent_id_in_csat",
        "agent_id_min_csat",
        "rn",
        "second_closed_at",
        "csat_sum_requester_id",
        "distinct_channel_in_closed_at_to_date",
        "csat_tercile",
        "requester_id_quartile",
        "rows_in_csat",
        "minutes_since_prev_opened_at",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
    .orderBy(
        F.col("priority").asc(),
        F.col("distinct_channel_in_closed_at_to_date").desc_nulls_first(),
        "id",
    )
    .limit(10)
)
