from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_channel_by_requeste = Window.partitionBy("priority", "channel").orderBy(F.col("requester_id_filled").desc(), "id").rowsBetween(-3, -1)
w_priority_channel_by_requeste_2 = Window.partitionBy("priority", "channel").orderBy(F.col("requester_id_filled").desc(), "id")
w_priority = Window.partitionBy("priority")
w_priority_channel_by_requeste_3 = Window.partitionBy("priority", "channel").orderBy(F.col("requester_id_filled").desc())

cleaned = (
    df_tickets
    .filter(F.col("priority").isin("P2", "P4"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
        F.lower(F.trim(F.col("priority"))).alias("priority_clean"),
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "priority_clean",
        F.max("requester_id_filled").over(w_priority_channel_by_requeste).alias("requester_id_filled_max_3_prior"),
        F.row_number().over(w_priority_channel_by_requeste_2).alias("requester_id_filled_seq"),
        F.round(F.col("requester_id_filled") / F.nullif(F.sum("requester_id_filled").over(w_priority), F.lit(0)), 4).alias("requester_id_filled_share_of_priority"),
        F.min("requester_id").over(w_priority).alias("priority_min_requester_id"),
        F.rank().over(w_priority_channel_by_requeste_3).alias("requester_id_filled_rank"),
    )
)

labeled = (
    with_windows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "priority_clean",
        "requester_id_filled_max_3_prior",
        "requester_id_filled_seq",
        "requester_id_filled_share_of_priority",
        "priority_min_requester_id",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
        F.lower(F.regexp_replace(F.trim(F.col("priority_clean")), " ", "_")).alias("priority_clean_slug"),
        F.greatest(F.col("requester_id_filled_rank"), F.col("requester_id")).alias("greatest_requester_id_filled_rank_requester_id"),
    )
)

per_group = (
    labeled
    .groupBy("channel", "agent_id")
    .agg(
        F.round(F.avg("requester_id_filled_seq"), 2).alias("avg_requester_id_filled_seq"),
        F.round(100 * F.count(F.when(F.col("priority") != "P1", 1)) / F.count("*"), 1).alias("pct_priority_p1"),
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.max("priority_min_requester_id").alias("max_priority_min_requester_id"),
        F.min("requester_id").alias("min_requester_id"),
    )
)

result = (
    per_group
    .select(
        "channel",
        "agent_id",
        "pct_priority_p1",
        "avg_requester_id_filled_seq",
    )
    .unpivot(
        ids=["channel", "agent_id"],
        values=["pct_priority_p1", "avg_requester_id_filled_seq"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .orderBy("channel", "agent_id", "field")
)
