from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_agent_id_by_requeste = Window.partitionBy("channel", "agent_id").orderBy(F.col("requester_id_per_csat").desc(), "id")
w_channel_by_opened_at_prior_3 = Window.partitionBy("channel").orderBy(F.col("opened_at").desc(), "id").rowsBetween(-3, -1)
w_channel_agent_id_by_requeste_2 = Window.partitionBy("channel", "agent_id").orderBy(F.col("requester_id_per_csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_month_channel = Window.partitionBy(F.month("opened_at"), "channel")
w_opened_at_year_by_requester = Window.partitionBy(F.year("opened_at")).orderBy(F.col("requester_id_per_csat").desc(), "id")
w_channel_by_opened_at_ties = Window.partitionBy("channel").orderBy(F.col("opened_at").desc())
w_channel_by_opened_at_prior_2 = Window.partitionBy("channel").orderBy(F.col("opened_at").desc(), "id").rowsBetween(-2, -1)
w_channel_agent_id_by_requeste_3 = Window.partitionBy("channel", "agent_id").orderBy(F.col("requester_id_per_csat").desc())
w_channel = Window.partitionBy("channel")
w_channel_agent_id_by_requeste_4 = Window.partitionBy("channel", "agent_id").orderBy(F.col("requester_id_filled").desc_nulls_first(), "id")
w_channel_by_opened_at = Window.partitionBy("channel").orderBy(F.col("opened_at").desc(), "id")

valid_tickets = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "bug")) & (F.col("channel") == "email"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "tags",
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
    )
)

metrics = (
    valid_tickets
    .select(
        "*",
        F.ntile(5).over(w_channel_agent_id_by_requeste).alias("requester_id_per_csat_quintile"),
        F.round(F.avg("requester_id_per_csat").over(w_channel_by_opened_at_prior_3), 2).alias("requester_id_per_csat_avg_3_prior"),
        F.nth_value("opened_at", 2).over(w_channel_agent_id_by_requeste_2).alias("second_opened_at"),
        F.size(F.collect_set("agent_id").over(w_opened_at_month_channel)).alias("distinct_agent_id_in_opened_at_month_channel"),
        F.lead("requester_id_filled").over(w_opened_at_year_by_requester).alias("next_requester_id_filled"),
        F.rank().over(w_channel_by_opened_at_ties).alias("opened_at_rank"),
        F.round(F.avg("requester_id_filled").over(w_channel_by_opened_at_prior_2), 2).alias("requester_id_filled_avg_2_prior"),
        F.round(F.percent_rank().over(w_channel_agent_id_by_requeste_3), 3).alias("requester_id_per_csat_percent_rank"),
        F.round(F.col("requester_id_filled") / F.nullif(F.sum("requester_id_filled").over(w_opened_at_month_channel), F.lit(0)), 4).alias("requester_id_filled_share_of_opened_at_month_cha"),
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
        F.round(F.percent_rank().over(w_channel_agent_id_by_requeste), 3).alias("requester_id_per_csat_percent_rank_2"),
        F.lead("requester_id_per_csat").over(w_channel_agent_id_by_requeste_4).alias("next_requester_id_per_csat"),
        F.lag("requester_id_filled").over(w_channel_by_opened_at).alias("prev_requester_id_filled"),
        F.round(F.cume_dist().over(w_channel_agent_id_by_requeste), 3).alias("requester_id_per_csat_cume_dist"),
        F.round(F.col("requester_id_per_csat") - F.avg("requester_id_per_csat").over(w_channel), 2).alias("requester_id_per_csat_vs_channel_avg"),
    )
)

result = (
    metrics
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "requester_id_filled",
        "requester_id_per_csat_quintile",
        "distinct_agent_id_in_opened_at_month_channel",
        "next_requester_id_filled",
        "opened_at_rank",
        "requester_id_filled_avg_2_prior",
        "requester_id_per_csat_percent_rank",
        "rows_in_channel",
        "requester_id_per_csat_percent_rank_2",
        "next_requester_id_per_csat",
        "prev_requester_id_filled",
        F.lower(F.trim(F.col("channel"))).alias("channel_clean"),
    )
    .orderBy(F.col("rows_in_channel").desc())
)
