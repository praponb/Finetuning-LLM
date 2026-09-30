from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_agent_hired_on = Window.orderBy(F.col("agent_hired_on").desc(), "id")
w_all_by_agent_hired_on_ties = Window.orderBy(F.col("agent_hired_on").desc())
w_opened_at_to_date_by_opened = Window.partitionBy(F.to_date("opened_at")).orderBy(F.col("opened_at").asc_nulls_last(), "id")
w_agent_id_by_opened_at_ties = Window.partitionBy("agent_id").orderBy("opened_at")
w_agent_id_by_opened_at = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_by_opened_at_full = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_tickets = (
    df_tickets
    .filter(F.col("requester_id") <= 18)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        F.initcap(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

combined_details = (
    recent_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.priority_clean",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

with_windows = (
    combined_details
    .select(
        "*",
        F.ntile(4).over(w_all_by_agent_hired_on).alias("agent_hired_on_quartile"),
        F.round(F.cume_dist().over(w_all_by_agent_hired_on_ties), 3).alias("agent_hired_on_cume_dist"),
        F.datediff(F.col("agent_hired_on"), F.lag("agent_hired_on").over(w_opened_at_to_date_by_opened)).alias("days_since_prev_agent_hired_on"),
        F.ntile(5).over(w_opened_at_to_date_by_opened).alias("opened_at_quintile"),
        F.dense_rank().over(w_agent_id_by_opened_at_ties).alias("opened_at_dense_rank"),
        F.round(F.percent_rank().over(w_agent_id_by_opened_at), 3).alias("opened_at_percent_rank"),
        F.round(F.percent_rank().over(w_opened_at_to_date_by_opened), 3).alias("opened_at_percent_rank_2"),
        F.rank().over(w_agent_id_by_opened_at_ties).alias("opened_at_rank"),
        F.row_number().over(w_agent_id_by_opened_at).alias("rn"),
        F.first("opened_at").over(w_agent_id_by_opened_at_full).alias("first_opened_at"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "agent_hired_on",
        "agent_tier",
        "days_since_prev_agent_hired_on",
        "opened_at_quintile",
        "opened_at_percent_rank",
        "opened_at_percent_rank_2",
        "opened_at_rank",
        "rn",
        F.coalesce(F.col("opened_at_dense_rank"), F.lit(0)).alias("opened_at_dense_rank_filled"),
    )
    .orderBy("agent_id", F.col("opened_at_percent_rank").desc_nulls_first())
)
