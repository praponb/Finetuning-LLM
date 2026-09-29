from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_csat_by_greatest_csa = Window.partitionBy("channel", "csat").orderBy("greatest_csat_requester_id", "id")
w_channel_agent_tier_by_csat_f = Window.partitionBy("channel", "agent_tier").orderBy(F.col("csat").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel = Window.partitionBy("channel")
w_agent_id_opened_at_month = Window.partitionBy("agent_id", F.month("opened_at"))
w_channel_csat_by_greatest_csa_2 = Window.partitionBy("channel", "csat").orderBy("greatest_csat_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_tier_by_greatest_csat = Window.partitionBy("agent_tier").orderBy("greatest_csat_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_opened_at = Window.orderBy("opened_at", "id")
w_agent_tier_by_greatest_csat_2 = Window.partitionBy("agent_tier").orderBy("greatest_csat_requester_id", "id").rowsBetween(-1, -1)

base_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        "tags",
        F.initcap(F.trim(F.col("priority"))).alias("priority_clean"),
        F.greatest(F.col("csat"), F.col("requester_id")).alias("greatest_csat_requester_id"),
    )
)

with_details = (
    base_tickets.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.hired_on").isNotNull()), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.csat",
        "s.greatest_csat_requester_id",
        F.col("su.tier").alias("agent_tier"),
    )
)

metrics = (
    with_details
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "greatest_csat_requester_id",
        "agent_tier",
        F.lead("csat").over(w_channel_csat_by_greatest_csa).alias("next_csat"),
        F.nth_value("requester_id", 2).over(w_channel_agent_tier_by_csat_f).alias("second_requester_id"),
        F.max("csat").over(w_channel).alias("channel_max_csat"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_channel), F.lit(0)), 4).alias("csat_share_of_channel"),
        F.round(F.col("greatest_csat_requester_id") / F.nullif(F.sum("greatest_csat_requester_id").over(w_agent_id_opened_at_month), F.lit(0)), 4).alias("greatest_csat_requester_id_share_of_agent_id_ope"),
        F.sum("greatest_csat_requester_id").over(w_channel_csat_by_greatest_csa_2).alias("running_greatest_csat_requester_id"),
        F.round(F.col("greatest_csat_requester_id") - F.avg("greatest_csat_requester_id").over(w_agent_id_opened_at_month), 2).alias("greatest_csat_requester_id_vs_agent_id_opened_at"),
        F.sum("greatest_csat_requester_id").over(w_agent_tier_by_greatest_csat).alias("running_greatest_csat_requester_id_2"),
        F.lag("csat").over(w_all_by_opened_at).alias("prev_csat"),
        F.max("requester_id").over(w_agent_tier_by_greatest_csat_2).alias("requester_id_max_1_prior"),
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
    )
)

summary = (
    metrics
    .groupBy("next_csat", "prev_csat")
    .agg(
        F.sum(F.when(F.col("agent_tier") != "T2", 1).otherwise(0)).alias("n_agent_tier_t2"),
        F.sum(F.when(F.col("channel") == "web", F.col("next_csat")).otherwise(0)).alias("next_csat_channel_web"),
        F.round(F.avg("next_csat"), 2).alias("avg_next_csat"),
        F.sum(F.when(F.col("channel").isNotNull(), 1).otherwise(0)).alias("n_channel_is_not_null"),
    )
)

result = (
    summary
    .filter(F.col("avg_next_csat") < 3.8)
    .select(
        "next_csat",
        "prev_csat",
        "n_agent_tier_t2",
        "next_csat_channel_web",
        "n_channel_is_not_null",
        F.round(F.col("n_agent_tier_t2") / F.nullif(F.col("next_csat"), F.lit(0)), 3).alias("n_agent_tier_t2_per_next_csat"),
    )
    .orderBy(
        F.col("n_channel_is_not_null").asc_nulls_last(),
        F.col("next_csat").desc(),
    )
)
