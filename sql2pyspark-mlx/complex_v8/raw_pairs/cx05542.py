from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_team_csat = Window.partitionBy("agent_team", "csat")
w_channel = Window.partitionBy("channel")
w_channel_csat_by_csat_full = Window.partitionBy("channel", "csat").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_team_csat_by_requester = Window.partitionBy("agent_team", "csat").orderBy(F.col("requester_id").desc(), "id")
w_all_by_requester_id_bucket = Window.orderBy(F.col("requester_id_bucket").desc(), "id")
w_all_by_csat = Window.orderBy(F.col("csat").desc(), "id")
w_channel_csat_by_csat_running = Window.partitionBy("channel", "csat").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_requester_id_bucket_decile_d = Window.partitionBy("requester_id_bucket_decile", "distinct_agent_team_in_channel").orderBy(F.col("distinct_agent_team_in_channel").desc(), "id")
w_requester_id_bucket_decile_d_2 = Window.partitionBy("requester_id_bucket_decile", "distinct_agent_team_in_channel").orderBy(F.col("distinct_agent_team_in_channel").desc(), "id").rowsBetween(-6, Window.currentRow)

scoped = (
    df_tickets
    .filter(F.month(F.col("opened_at")).isin(2, 3))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
    )
)

with_details_agent = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.requester_id_bucket",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

ranked = (
    with_details_agent
    .select(
        "id",
        "requester_id",
        "channel",
        "requester_id_bucket",
        "agent_team",
        F.count(F.lit(1)).over(w_agent_team_csat).alias("rows_in_agent_team_csat"),
        F.size(F.collect_set("agent_team").over(w_channel)).alias("distinct_agent_team_in_channel"),
        F.max("requester_id_bucket").over(w_agent_team_csat).alias("agent_team_csat_max_requester_id_bucket"),
        F.last("csat").over(w_channel_csat_by_csat_full).alias("last_csat"),
        F.row_number().over(w_agent_team_csat_by_requester).alias("requester_id_seq"),
        F.ntile(10).over(w_all_by_requester_id_bucket).alias("requester_id_bucket_decile"),
        F.size(F.collect_set("agent_id").over(w_agent_team_csat)).alias("distinct_agent_id_in_agent_team_csat"),
        F.row_number().over(w_all_by_csat).alias("rn"),
        F.last("requester_id_bucket", ignorenulls=True).over(w_channel_csat_by_csat_running).alias("last_known_requester_id_bucket"),
    )
)

annotated = (
    ranked
    .select(
        "id",
        "requester_id",
        "requester_id_bucket",
        "agent_team",
        "rows_in_agent_team_csat",
        "distinct_agent_team_in_channel",
        "agent_team_csat_max_requester_id_bucket",
        "requester_id_bucket_decile",
        "distinct_agent_id_in_agent_team_csat",
        "last_known_requester_id_bucket",
        F.greatest(F.col("requester_id_bucket"), F.col("requester_id_seq")).alias("greatest_requester_id_bucket_requester_id_seq"),
        (F.floor(F.col("requester_id_bucket") / 5) * 5).alias("requester_id_bucket_bucket"),
        (F.floor(F.col("rn") / 2) * 2).alias("rn_bucket"),
    )
)

with_windows = (
    annotated
    .select(
        "id",
        "requester_id",
        "agent_team",
        "rows_in_agent_team_csat",
        "requester_id_bucket_decile",
        "distinct_agent_id_in_agent_team_csat",
        "last_known_requester_id_bucket",
        "greatest_requester_id_bucket_requester_id_seq",
        "requester_id_bucket_bucket",
        "rn_bucket",
        F.ntile(10).over(w_requester_id_bucket_decile_d).alias("distinct_agent_team_in_channel_decile"),
        F.lead("requester_id_bucket_decile", 2).over(w_requester_id_bucket_decile_d).alias("next2_requester_id_bucket_decile"),
        F.sum("requester_id_bucket_decile").over(w_requester_id_bucket_decile_d_2).alias("requester_id_bucket_decile_sum_7"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "agent_team",
        "rows_in_agent_team_csat",
        "requester_id_bucket_decile",
        "distinct_agent_id_in_agent_team_csat",
        "last_known_requester_id_bucket",
        "greatest_requester_id_bucket_requester_id_seq",
        "requester_id_bucket_bucket",
        "distinct_agent_team_in_channel_decile",
        "next2_requester_id_bucket_decile",
        "requester_id_bucket_decile_sum_7",
        F.when(F.col("agent_team").isin("Onboarding", "VIP"), "group_a").when(F.col("agent_team") == "Billing", "group_b").otherwise("other").alias("agent_team_group"),
    )
    .orderBy(
        F.col("rows_in_agent_team_csat").desc_nulls_first(),
        "distinct_agent_team_in_channel_decile",
    )
)
