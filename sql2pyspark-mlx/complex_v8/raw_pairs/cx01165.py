from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_month_channel_by_c = Window.partitionBy(F.date_trunc("month", "closed_at"), "channel").orderBy("closed_at", "id")
w_closed_at_year_by_closed_at = Window.partitionBy(F.year("closed_at")).orderBy(F.col("closed_at").desc(), "id")
w_agent_tier_by_closed_at = Window.partitionBy("agent_tier").orderBy("closed_at", "id")
w_closed_at_month_channel_by_c_2 = Window.partitionBy(F.date_trunc("month", "closed_at"), "channel").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_tickets
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        F.concat_ws(": ", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

with_parent = (
    base.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.channel",
        "s.channel_priority_label",
        F.col("su.tier").alias("agent_tier"),
    )
)

with_windows = (
    with_parent
    .select(
        "id",
        "agent_id",
        "channel",
        "agent_tier",
        F.lead("closed_at", 2).over(w_closed_at_month_channel_by_c).alias("next2_closed_at"),
        F.dense_rank().over(w_closed_at_year_by_closed_at).alias("closed_at_dense_rank"),
        F.lag("opened_at").over(w_closed_at_year_by_closed_at).alias("prev_opened_at"),
        F.row_number().over(w_agent_tier_by_closed_at).alias("seq_no"),
        F.row_number().over(w_closed_at_year_by_closed_at).alias("closed_at_seq"),
        F.first("opened_at", ignorenulls=True).over(w_closed_at_month_channel_by_c_2).alias("first_known_opened_at"),
        F.ntile(5).over(w_closed_at_month_channel_by_c).alias("closed_at_quintile"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "agent_id",
        "channel",
        "closed_at_dense_rank",
        "closed_at_seq",
        "first_known_opened_at",
        "closed_at_quintile",
    )
    .orderBy(F.col("closed_at_dense_rank").asc(), F.col("agent_id").desc())
)
