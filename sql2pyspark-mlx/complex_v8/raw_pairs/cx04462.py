from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_opened_at_month_by_o = Window.partitionBy("channel", F.date_trunc("month", "opened_at")).orderBy(F.col("opened_at").asc_nulls_last(), "id")
w_closed_at_month_channel_by_o = Window.partitionBy(F.month("closed_at"), "channel").orderBy("opened_at", "id")
w_priority = Window.partitionBy("priority")
w_agent_id_by_csat = Window.partitionBy("agent_id").orderBy(F.col("csat").desc_nulls_first(), "id")
w_agent_id = Window.partitionBy("agent_id")
w_channel_opened_at_month_by_o_2 = Window.partitionBy("channel", F.date_trunc("month", "opened_at")).orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_tickets
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.lower(F.trim(F.col("priority"))).alias("priority_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
)

sequenced = (
    cleaned
    .select(
        "*",
        F.lead("closed_at").over(w_channel_opened_at_month_by_o).alias("next_closed_at"),
        F.lead("csat", 3).over(w_closed_at_month_channel_by_o).alias("next3_csat"),
        F.round(F.col("csat") - F.avg("csat").over(w_priority), 2).alias("csat_vs_priority_avg"),
        F.ntile(5).over(w_channel_opened_at_month_by_o).alias("opened_at_quintile"),
        F.row_number().over(w_agent_id_by_csat).alias("seq_no"),
        F.row_number().over(w_closed_at_month_channel_by_o).alias("rn"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_agent_id), F.lit(0)), 4).alias("csat_share_of_agent_id"),
        F.count(F.lit(1)).over(w_priority).alias("rows_in_priority"),
        F.nth_value("opened_at", 2).over(w_channel_opened_at_month_by_o_2).alias("second_opened_at"),
    )
)

top_per_group = (
    sequenced
    .filter(F.col("seq_no") <= 2)
    .drop("seq_no")
)

result = (
    top_per_group
    .select(
        "id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "channel_slug",
        "next_closed_at",
        "next3_csat",
        "opened_at_quintile",
        "csat_share_of_agent_id",
        F.round((F.unix_timestamp(F.col("second_opened_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600, 1).alias("hours_opened_at_to_second_opened_at"),
        F.concat_ws(" / ", F.col("channel_slug"), F.col("priority_clean")).alias("channel_slug_priority_clean_label"),
    )
    .orderBy(F.col("next_closed_at").asc_nulls_last(), F.col("id").desc())
)
