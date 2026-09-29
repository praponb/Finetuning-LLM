from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_agent_id_by_closed_at_f = Window.partitionBy("csat", "agent_id").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_agent_id_by_closed_at = Window.partitionBy("csat", "agent_id").orderBy(F.col("closed_at").desc(), "id")
w_all_by_minutes_opened_at_to = Window.orderBy(F.col("minutes_opened_at_to_closed_at").desc(), "id")
w_all_by_minutes_opened_at_to_2 = Window.orderBy(F.col("minutes_opened_at_to_closed_at").desc())
w_all_by_opened_at = Window.orderBy("opened_at", "id")
w_priority = Window.partitionBy("priority")
w_csat_by_minutes_opened_at_to = Window.partitionBy("csat").orderBy("minutes_opened_at_to_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_month_agent_id_by = Window.partitionBy(F.date_trunc("month", "opened_at"), "agent_id").orderBy("opened_at_percent_rank", "id")
w_priority_csat_by_rows_in_pri = Window.partitionBy("priority", "csat").orderBy("rows_in_priority", "id")
w_last_opened_at_month = Window.partitionBy(F.date_trunc("month", "last_opened_at"))
w_minutes_opened_at_to_closed = Window.partitionBy("minutes_opened_at_to_closed_at_rank")
w_last_opened_at_year_by_last = Window.partitionBy(F.year("last_opened_at")).orderBy("last_opened_at_month_max_hours_closed_at_to_open", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_tickets
    .filter(F.col("requester_id") <= 18)
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
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 3600, 1).alias("hours_closed_at_to_opened_at"),
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
    )
)

with_windows = (
    scoped
    .select(
        "*",
        F.last("opened_at").over(w_csat_agent_id_by_closed_at_f).alias("last_opened_at"),
        F.lag("opened_at", 3).over(w_csat_agent_id_by_closed_at).alias("prev3_opened_at"),
        F.ntile(3).over(w_all_by_minutes_opened_at_to).alias("minutes_opened_at_to_closed_at_tercile"),
        F.rank().over(w_all_by_minutes_opened_at_to_2).alias("minutes_opened_at_to_closed_at_rank"),
    )
)

windowed = (
    with_windows
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        "hours_closed_at_to_opened_at",
        "minutes_opened_at_to_closed_at",
        "last_opened_at",
        "prev3_opened_at",
        "minutes_opened_at_to_closed_at_rank",
        F.round(F.percent_rank().over(w_all_by_opened_at), 3).alias("opened_at_percent_rank"),
        F.count(F.lit(1)).over(w_priority).alias("rows_in_priority"),
        F.max("requester_id").over(w_priority).alias("priority_max_requester_id"),
        F.min("hours_closed_at_to_opened_at").over(w_priority).alias("priority_min_hours_closed_at_to_opened_at"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.last("priority_max_requester_id").over(w_csat_by_minutes_opened_at_to).alias("last_priority_max_requester_id"),
        F.lag("prev3_opened_at").over(w_opened_at_month_agent_id_by).alias("prev_prev3_opened_at"),
        F.round(F.cume_dist().over(w_priority_csat_by_rows_in_pri), 3).alias("rows_in_priority_cume_dist"),
        F.max("hours_closed_at_to_opened_at").over(w_last_opened_at_month).alias("last_opened_at_month_max_hours_closed_at_to_open"),
    )
)

sequenced = (
    with_trends
    .select(
        "id",
        "opened_at",
        "priority",
        "last_opened_at",
        "opened_at_percent_rank",
        "rows_in_priority",
        "priority_max_requester_id",
        "last_priority_max_requester_id",
        "prev_prev3_opened_at",
        "rows_in_priority_cume_dist",
        F.round(F.col("opened_at_percent_rank") - F.avg("opened_at_percent_rank").over(w_minutes_opened_at_to_closed), 2).alias("opened_at_percent_rank_vs_minutes_opened_at_to_c"),
        F.last("opened_at", ignorenulls=True).over(w_last_opened_at_year_by_last).alias("last_known_opened_at"),
        F.max("priority_max_requester_id").over(w_last_opened_at_year_by_last).alias("max_priority_max_requester_id_so_far"),
        F.round(F.col("rows_in_priority") - F.avg("rows_in_priority").over(w_minutes_opened_at_to_closed), 2).alias("rows_in_priority_vs_minutes_opened_at_to_closed_"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "opened_at_percent_rank_vs_minutes_opened_at_to_c",
        "rows_in_priority_cume_dist",
        "opened_at_percent_rank",
    )
    .unpivot(
        ids=["id"],
        values=["opened_at_percent_rank_vs_minutes_opened_at_to_c", "rows_in_priority_cume_dist", "opened_at_percent_rank"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "measure")
)
