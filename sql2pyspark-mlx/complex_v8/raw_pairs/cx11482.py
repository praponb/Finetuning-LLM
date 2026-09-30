from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_agent_hired_on_year = Window.partitionBy("csat", F.year("agent_hired_on"))
w_agent_hired_on_month_by_agen = Window.partitionBy(F.month("agent_hired_on")).orderBy("agent_hired_on", "id")
w_channel_closed_at_month_by_c = Window.partitionBy("channel", F.date_trunc("month", "closed_at")).orderBy(F.col("csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_closed_at_month_by_c_2 = Window.partitionBy("channel", F.date_trunc("month", "closed_at")).orderBy(F.col("csat").asc_nulls_last(), "id")
w_all_by_opened_at_full = Window.orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_month = Window.partitionBy(F.month("opened_at"))
w_channel_agent_id_by_closed_a = Window.partitionBy("channel", "agent_id").orderBy("closed_at")
w_channel_agent_id_by_closed_a_2 = Window.partitionBy("channel", "agent_id").orderBy("closed_at", "id")
w_channel_closed_at_month_by_c_3 = Window.partitionBy("channel", F.date_trunc("month", "closed_at")).orderBy(F.col("csat").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_csat_by_requester_id_minus_c = Window.partitionBy("csat").orderBy("requester_id_minus_csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_by_requester_id_minus_c_2 = Window.partitionBy("csat").orderBy("requester_id_minus_csat", "id")
w_csat_quintile_by_rows_in_csa = Window.partitionBy("csat_quintile").orderBy("rows_in_csat_agent_hired_on_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_by_requester_id_minus_c_3 = Window.partitionBy("csat").orderBy("requester_id_minus_csat")

valid_tickets = (
    df_tickets
    .filter(F.col("requester_id") < 18)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        (F.col("requester_id") - F.col("csat")).alias("requester_id_minus_csat"),
        F.array_contains(F.col("tags"), "login").alias("has_login"),
    )
)

with_details = (
    valid_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.requester_id_minus_csat",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

with_trends = (
    with_details
    .select(
        "*",
        F.count(F.lit(1)).over(w_csat_agent_hired_on_year).alias("rows_in_csat_agent_hired_on_year"),
        F.round(F.cume_dist().over(w_agent_hired_on_month_by_agen), 3).alias("agent_hired_on_cume_dist"),
        F.size(F.collect_set("agent_team").over(w_csat_agent_hired_on_year)).alias("distinct_agent_team_in_csat_agent_hired_on_year"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_csat_agent_hired_on_year), F.lit(0)), 4).alias("csat_share_of_csat_agent_hired_on_year"),
        F.last("opened_at", ignorenulls=True).over(w_channel_closed_at_month_by_c).alias("last_known_opened_at"),
        F.ntile(5).over(w_channel_closed_at_month_by_c_2).alias("csat_quintile"),
        F.nth_value("closed_at", 2).over(w_all_by_opened_at_full).alias("second_closed_at"),
        F.count(F.lit(1)).over(w_opened_at_month).alias("rows_in_opened_at_month"),
        F.round(F.percent_rank().over(w_channel_agent_id_by_closed_a), 3).alias("closed_at_percent_rank"),
        F.dense_rank().over(w_channel_agent_id_by_closed_a_2).alias("closed_at_dense_rank"),
        F.round(F.col("csat") - F.avg("csat").over(w_csat_agent_hired_on_year), 2).alias("csat_vs_csat_agent_hired_on_year_avg"),
        F.round(F.avg("csat").over(w_channel_closed_at_month_by_c_3), 2).alias("csat_avg_7"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_channel_agent_id_by_closed_a_2))) / 60, 1).alias("minutes_since_prev_opened_at"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "id",
        "agent_id",
        "closed_at",
        "csat",
        "requester_id_minus_csat",
        "agent_team",
        "agent_hired_on",
        "rows_in_csat_agent_hired_on_year",
        "agent_hired_on_cume_dist",
        "distinct_agent_team_in_csat_agent_hired_on_year",
        "last_known_opened_at",
        "csat_quintile",
        "second_closed_at",
        "rows_in_opened_at_month",
        "closed_at_percent_rank",
        "closed_at_dense_rank",
        "csat_vs_csat_agent_hired_on_year_avg",
        "csat_avg_7",
        "minutes_since_prev_opened_at",
        F.first("minutes_since_prev_opened_at", ignorenulls=True).over(w_csat_by_requester_id_minus_c).alias("first_known_minutes_since_prev_opened_at"),
        F.row_number().over(w_csat_by_requester_id_minus_c_2).alias("requester_id_minus_csat_seq"),
        F.last("requester_id_minus_csat", ignorenulls=True).over(w_csat_quintile_by_rows_in_csa).alias("last_known_requester_id_minus_csat"),
        F.round(F.percent_rank().over(w_csat_by_requester_id_minus_c_3), 3).alias("requester_id_minus_csat_percent_rank"),
        F.lag("closed_at_dense_rank").over(w_csat_by_requester_id_minus_c_2).alias("prev_closed_at_dense_rank"),
        F.ntile(10).over(w_csat_by_requester_id_minus_c_2).alias("requester_id_minus_csat_decile"),
    )
)

result = (
    with_trends_2
    .select(
        "id",
        "closed_at",
        "csat",
        "agent_team",
        "agent_hired_on",
        "rows_in_csat_agent_hired_on_year",
        "distinct_agent_team_in_csat_agent_hired_on_year",
        "last_known_opened_at",
        "second_closed_at",
        "rows_in_opened_at_month",
        "closed_at_percent_rank",
        "closed_at_dense_rank",
        "csat_vs_csat_agent_hired_on_year_avg",
        "csat_avg_7",
        "minutes_since_prev_opened_at",
        "first_known_minutes_since_prev_opened_at",
        "requester_id_minus_csat_seq",
        "last_known_requester_id_minus_csat",
        "requester_id_minus_csat_percent_rank",
        "requester_id_minus_csat_decile",
        F.date_trunc("hour", F.col("second_closed_at")).alias("second_closed_at_hour"),
    )
    .orderBy(
        F.col("closed_at_dense_rank").desc(),
        F.col("second_closed_at_hour").asc(),
    )
)
