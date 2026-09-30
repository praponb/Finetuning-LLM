from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_minutes_opened_a = Window.partitionBy("agent_id").orderBy("minutes_opened_at_to_closed_at", "id")
w_agent_id_by_minutes_opened_a_2 = Window.partitionBy("agent_id").orderBy("minutes_opened_at_to_closed_at")
w_channel_by_minutes_opened_at = Window.partitionBy("channel").orderBy(F.col("minutes_opened_at_to_closed_at").desc_nulls_first())
w_agent_id_channel_by_minutes = Window.partitionBy("agent_id", "channel").orderBy(F.col("minutes_opened_at_to_closed_at").desc_nulls_first())
w_agent_id_channel_by_minutes_2 = Window.partitionBy("agent_id", "channel").orderBy(F.col("minutes_opened_at_to_closed_at").desc_nulls_first(), "id")

valid_tickets = (
    df_tickets
    .filter(F.col("csat") <= 3)
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "tags",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
    )
)

ranked = (
    valid_tickets
    .select(
        "*",
        F.rank().over(w_agent_id_by_minutes_opened_a).alias("minutes_opened_at_to_closed_at_rank"),
        F.round(F.cume_dist().over(w_agent_id_by_minutes_opened_a_2), 3).alias("minutes_opened_at_to_closed_at_cume_dist"),
        F.round(F.cume_dist().over(w_channel_by_minutes_opened_at), 3).alias("minutes_opened_at_to_closed_at_cume_dist_2"),
        F.ntile(3).over(w_agent_id_by_minutes_opened_a).alias("minutes_opened_at_to_closed_at_tercile"),
        F.row_number().over(w_agent_id_by_minutes_opened_a).alias("seq_no"),
        F.round(F.percent_rank().over(w_agent_id_channel_by_minutes), 3).alias("minutes_opened_at_to_closed_at_percent_rank"),
        F.rank().over(w_channel_by_minutes_opened_at).alias("minutes_opened_at_to_closed_at_rank_2"),
        F.row_number().over(w_agent_id_channel_by_minutes_2).alias("minutes_opened_at_to_closed_at_seq"),
    )
)

top_rows = ranked.filter(F.col("minutes_opened_at_to_closed_at_rank") == 1)

stats = (
    top_rows
    .groupBy("minutes_opened_at_to_closed_at_rank_2", "agent_id")
    .agg(
        F.round(F.sum("minutes_opened_at_to_closed_at_tercile") / F.nullif(F.sum("minutes_opened_at_to_closed_at_rank_2"), F.lit(0)), 3).alias("minutes_opened_at_to_closed_at_tercile_to_minute"),
        F.sum(F.when(F.col("channel").isNotNull(), 1).otherwise(0)).alias("n_channel_is_not_null"),
        F.max("minutes_opened_at_to_closed_at_seq").alias("max_minutes_opened_at_to_closed_at_seq_check"),
    )
    .filter(F.col("max_minutes_opened_at_to_closed_at_seq_check") > 2)
    .drop("max_minutes_opened_at_to_closed_at_seq_check")
)

result = (
    stats
    .select(
        "minutes_opened_at_to_closed_at_rank_2",
        "minutes_opened_at_to_closed_at_tercile_to_minute",
        "n_channel_is_not_null",
        F.coalesce(F.col("n_channel_is_not_null"), F.lit(0)).alias("n_channel_is_not_null_filled"),
        F.abs(F.col("n_channel_is_not_null") - 3).alias("n_channel_is_not_null_distance"),
    )
    .orderBy(
        F.col("n_channel_is_not_null_distance").desc(),
        F.col("minutes_opened_at_to_closed_at_tercile_to_minute").desc(),
    )
)
