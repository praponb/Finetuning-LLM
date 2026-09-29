from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority")
w_closed_at_to_date = Window.partitionBy(F.to_date("closed_at"))
w_closed_at_year_agent_id_by_r = Window.partitionBy(F.year("closed_at"), "agent_id").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel_agent_id_by_requeste = Window.partitionBy("channel", "agent_id").orderBy(F.col("requester_id").desc(), "id").rowsBetween(-1, Window.currentRow)
w_channel_by_closed_at = Window.partitionBy("channel").orderBy(F.col("closed_at").desc(), "id")
w_channel_by_closed_at_running = Window.partitionBy("channel").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_closed_at_year_agent_id_by_r_2 = Window.partitionBy(F.year("closed_at"), "agent_id").orderBy("requester_id", "id").rowsBetween(-1, Window.currentRow)
w_priority_by_requester_id_sum = Window.partitionBy("priority").orderBy("requester_id_sum_2", "id")
w_agent_id_by_priority_sum_req = Window.partitionBy("agent_id").orderBy(F.col("priority_sum_requester_id").desc(), "id")
w_first_known_closed_at_month = Window.partitionBy(F.month("first_known_closed_at"), "priority")
w_second_closed_at_to_date_clo = Window.partitionBy(F.to_date("second_closed_at"), F.to_date("closed_at")).orderBy("requester_id", "id").rowsBetween(-6, Window.currentRow)
w_agent_id_by_priority_sum_req_2 = Window.partitionBy("agent_id").orderBy(F.col("priority_sum_requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_second_closed_at_to_date_clo_2 = Window.partitionBy(F.to_date("second_closed_at"), F.to_date("closed_at")).orderBy("requester_id", "id")

valid_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        F.lower(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

sequenced = (
    valid_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "priority_clean",
        F.round(F.avg("requester_id").over(w_priority), 2).alias("priority_avg_requester_id"),
        F.sum("requester_id").over(w_closed_at_to_date).alias("closed_at_to_date_sum_requester_id"),
        F.sum("requester_id").over(w_priority).alias("priority_sum_requester_id"),
        F.nth_value("closed_at", 2).over(w_closed_at_year_agent_id_by_r).alias("second_closed_at"),
        F.round(F.avg("requester_id").over(w_channel_agent_id_by_requeste), 2).alias("requester_id_avg_2"),
        F.rank().over(w_channel_by_closed_at).alias("closed_at_rank"),
        F.first("closed_at", ignorenulls=True).over(w_channel_by_closed_at_running).alias("first_known_closed_at"),
        F.sum("requester_id").over(w_closed_at_year_agent_id_by_r_2).alias("requester_id_sum_2"),
        (F.col("requester_id") - F.lag("requester_id").over(w_channel_by_closed_at)).alias("requester_id_change"),
    )
)

leaders = (
    sequenced
    .filter(F.col("closed_at_rank") == 1)
    .drop("closed_at_rank")
)

windowed = (
    leaders
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "priority_clean",
        "priority_avg_requester_id",
        "closed_at_to_date_sum_requester_id",
        "priority_sum_requester_id",
        "second_closed_at",
        "first_known_closed_at",
        "requester_id_sum_2",
        "requester_id_change",
        F.round(F.cume_dist().over(w_priority_by_requester_id_sum), 3).alias("requester_id_sum_2_cume_dist"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_agent_id_by_priority_sum_req))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.size(F.collect_set("agent_id").over(w_first_known_closed_at_month)).alias("distinct_agent_id_in_first_known_closed_at_month"),
        F.max("requester_id_avg_2").over(w_second_closed_at_to_date_clo).alias("requester_id_avg_2_max_7"),
        F.last("first_known_closed_at", ignorenulls=True).over(w_agent_id_by_priority_sum_req_2).alias("last_known_first_known_closed_at"),
        F.count(F.lit(1)).over(w_first_known_closed_at_month).alias("rows_in_first_known_closed_at_month_priority"),
        F.lag("requester_id_sum_2", 2, 0).over(w_second_closed_at_to_date_clo_2).alias("prev2_requester_id_sum_2"),
    )
)

result = (
    windowed
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "priority_clean",
        "priority_avg_requester_id",
        "closed_at_to_date_sum_requester_id",
        "priority_sum_requester_id",
        "requester_id_sum_2",
        "minutes_since_prev_closed_at",
        "distinct_agent_id_in_first_known_closed_at_month",
        "last_known_first_known_closed_at",
        "rows_in_first_known_closed_at_month_priority",
        "prev2_requester_id_sum_2",
        F.substring(F.trim(F.col("priority")), 1, 4).alias("priority_prefix"),
        F.concat(F.col("channel"), F.lit("#"), F.col("requester_id").cast("string")).alias("channel_ref"),
    )
    .orderBy(F.col("requester_id").desc_nulls_first())
)
