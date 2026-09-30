from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_channel_by_opened_a = Window.partitionBy("agent_id", "channel").orderBy(F.col("opened_at").asc_nulls_last(), "id")
w_csat = Window.partitionBy("csat")
w_agent_id_channel_by_opened_a_2 = Window.partitionBy("agent_id", "channel").orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_tickets = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        "tags",
        F.concat(F.col("channel"), F.lit("#"), F.col("agent_id").cast("string")).alias("channel_ref"),
        (F.col("csat") + F.col("requester_id")).alias("csat_plus_requester_id"),
    )
)

sequenced = (
    prepared_tickets
    .select(
        "id",
        "agent_id",
        "csat",
        "tags",
        "channel_ref",
        "csat_plus_requester_id",
        F.ntile(3).over(w_agent_id_channel_by_opened_a).alias("opened_at_tercile"),
        F.sum("csat").over(w_csat).alias("csat_sum_csat"),
        F.round(F.avg("csat").over(w_csat), 2).alias("csat_avg_csat"),
        F.row_number().over(w_agent_id_channel_by_opened_a).alias("opened_at_seq"),
        F.nth_value("csat", 2).over(w_agent_id_channel_by_opened_a_2).alias("second_csat"),
        F.round(F.col("csat_plus_requester_id") - F.avg("csat_plus_requester_id").over(w_csat), 2).alias("csat_plus_requester_id_vs_csat_avg"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["opened_at_tercile", "opened_at_seq"], ["opened_at_tercile"], []],
        "opened_at_tercile", "opened_at_seq",
    )
    .agg(
        F.grouping("opened_at_tercile").alias("is_total_opened_at_tercile"),
        F.round(F.sum("csat_avg_csat"), 2).alias("total_csat_avg_csat"),
    )
    .orderBy(
        F.col("opened_at_tercile").asc_nulls_last(),
        F.col("opened_at_seq").asc_nulls_last(),
    )
)
