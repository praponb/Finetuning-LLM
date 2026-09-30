from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_channel_by_requeste = Window.partitionBy("agent_id", "channel").orderBy(F.col("requester_id").desc(), "id")
w_agent_id_by_requester_id_per = Window.partitionBy("agent_id").orderBy(F.col("requester_id_per_csat").desc(), "id")
w_all_by_requester_id = Window.orderBy(F.col("requester_id").desc_nulls_first(), "id")
w_channel_agent_id = Window.partitionBy("channel", "agent_id")
w_channel_by_requester_id_per = Window.partitionBy("channel").orderBy(F.col("requester_id_per_csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_channel_by_requeste_2 = Window.partitionBy("agent_id", "channel").orderBy(F.col("requester_id").desc(), "id").rowsBetween(-2, -1)
w_all_by_requester_id_full = Window.orderBy(F.col("requester_id").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_tickets
    .filter((F.col("csat") < 5) & (F.col("requester_id").between(7, 23)))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "tags",
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
    )
)

sequenced = (
    scoped
    .select(
        "id",
        "channel",
        "tags",
        "requester_id_per_csat",
        F.row_number().over(w_agent_id_channel_by_requeste).alias("requester_id_seq"),
        F.lead("requester_id").over(w_agent_id_by_requester_id_per).alias("next_requester_id"),
        (F.col("requester_id") - F.lag("requester_id").over(w_all_by_requester_id)).alias("requester_id_change"),
        F.min("requester_id_per_csat").over(w_channel_agent_id).alias("channel_agent_id_min_requester_id_per_csat"),
        F.max("requester_id").over(w_channel_agent_id).alias("channel_agent_id_max_requester_id"),
        F.nth_value("requester_id_per_csat", 2).over(w_channel_by_requester_id_per).alias("second_requester_id_per_csat"),
        F.round(F.cume_dist().over(w_agent_id_by_requester_id_per), 3).alias("requester_id_per_csat_cume_dist"),
        F.sum("requester_id").over(w_agent_id_channel_by_requeste_2).alias("requester_id_sum_2_prior"),
        F.nth_value("requester_id_per_csat", 2).over(w_all_by_requester_id_full).alias("second_requester_id_per_csat_2"),
    )
)

result = (
    sequenced
    .filter(F.col("requester_id_per_csat_cume_dist").between(0.2, 0.7))
    .select(
        "id",
        "channel",
        "requester_id_per_csat",
        "next_requester_id",
        "requester_id_change",
        "channel_agent_id_min_requester_id_per_csat",
        "second_requester_id_per_csat",
        "requester_id_per_csat_cume_dist",
        "requester_id_sum_2_prior",
        "second_requester_id_per_csat_2",
        F.when(F.col("channel").isin("chat", "email"), "group_a").when(F.col("channel") == "web", "group_b").otherwise("other").alias("channel_group"),
        F.round(F.col("requester_id_sum_2_prior") / F.nullif(F.col("next_requester_id"), F.lit(0)), 3).alias("requester_id_sum_2_prior_per_next_requester_id"),
    )
    .orderBy(
        F.col("channel_agent_id_min_requester_id_per_csat").desc(),
        F.col("id").desc_nulls_first(),
    )
)
