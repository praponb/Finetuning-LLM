from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_hired_on_month = Window.partitionBy(F.date_trunc("month", "agent_hired_on"))
w_agent_id_opened_at_month_by = Window.partitionBy("agent_id", F.month("opened_at")).orderBy("opened_at")
w_opened_at_dense_rank_opened = Window.partitionBy("opened_at_dense_rank", F.year("opened_at"))
w_priority_opened_at_month_by = Window.partitionBy("priority", F.month("opened_at")).orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_tickets = (
    df_tickets
    .filter((F.col("channel").isin("chat", "web", "phone")) | (F.col("agent_id") >= 6))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        F.initcap(F.trim(F.col("priority"))).alias("priority_clean"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
        F.concat_ws(": ", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

scored = (
    base_tickets
    .select(
        "*",
        F.round(F.col("days_since_opened_at") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("days_since_opened_at_per_requester_id"),
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
    )
)

enriched = (
    scored.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.priority_clean",
        "s.days_since_opened_at",
        "s.channel_priority_label",
        "s.days_since_opened_at_per_requester_id",
        "s.minutes_opened_at_to_closed_at",
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

annotated = (
    enriched
    .filter(F.col("days_since_opened_at") < 420)
    .withColumn("channel_priority_label_ref", F.concat(F.col("channel_priority_label"), F.lit("#"), F.col("requester_id").cast("string")))
)

with_windows = (
    annotated
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "priority_clean",
        "days_since_opened_at",
        "channel_priority_label",
        "days_since_opened_at_per_requester_id",
        "minutes_opened_at_to_closed_at",
        "agent_hired_on",
        "agent_name",
        "channel_priority_label_ref",
        F.count(F.lit(1)).over(w_agent_hired_on_month).alias("rows_in_agent_hired_on_month"),
        F.dense_rank().over(w_agent_id_opened_at_month_by).alias("opened_at_dense_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_opened_at_dense_rank_opened), F.lit(0)), 4).alias("requester_id_share_of_opened_at_dense_rank_opene"),
        F.size(F.collect_set("priority").over(w_opened_at_dense_rank_opened)).alias("distinct_priority_in_opened_at_dense_rank_opened"),
        F.last("days_since_opened_at", ignorenulls=True).over(w_priority_opened_at_month_by).alias("last_known_days_since_opened_at"),
        F.round(F.col("days_since_opened_at") - F.avg("days_since_opened_at").over(w_opened_at_dense_rank_opened), 2).alias("days_since_opened_at_vs_opened_at_dense_rank_ope"),
        F.round(F.col("days_since_opened_at") / F.nullif(F.sum("days_since_opened_at").over(w_opened_at_dense_rank_opened), F.lit(0)), 4).alias("days_since_opened_at_share_of_opened_at_dense_ra"),
    )
)

result = (
    metrics
    .filter(F.col("opened_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "days_since_opened_at_per_requester_id",
        "minutes_opened_at_to_closed_at",
        "agent_hired_on",
        "channel_priority_label_ref",
        "rows_in_agent_hired_on_month",
        "requester_id_share_of_opened_at_dense_rank_opene",
        "distinct_priority_in_opened_at_dense_rank_opened",
        "last_known_days_since_opened_at",
        "days_since_opened_at_vs_opened_at_dense_rank_ope",
        "days_since_opened_at_share_of_opened_at_dense_ra",
        F.concat(F.col("agent_name"), F.lit("#"), F.col("opened_at_dense_rank").cast("string")).alias("agent_name_ref"),
        F.concat_ws("-", F.col("channel_priority_label"), F.col("agent_name")).alias("channel_priority_label_agent_name_label"),
    )
)
