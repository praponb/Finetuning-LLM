from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_channel_in_chat_web_phone = Window.partitionBy("n_channel_in_chat_web_phone").orderBy("cnt", "opened_at_month")
w_cnt_distinct_agent_ids_by_cl = Window.partitionBy("cnt", "distinct_agent_ids").orderBy("closed_at_hour_to_csat_filled_ratio")

recent_tickets = (
    df_tickets
    .filter((F.col("requester_id") >= 7) & (F.col("opened_at") < F.lit("2025-03-02 08:00:00").cast("timestamp")))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
        F.date_trunc("week", F.col("closed_at")).alias("closed_at_week"),
        F.hour(F.col("closed_at")).alias("closed_at_hour"),
    )
)

totals = (
    recent_tickets
    .groupBy(F.date_trunc("month", "opened_at").alias("opened_at_month"))
    .agg(
        F.round(F.sum("closed_at_hour") / F.nullif(F.sum("csat_filled"), F.lit(0)), 3).alias("closed_at_hour_to_csat_filled_ratio"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.count(F.when(F.col("channel").isin("chat", "web", "phone"), 1)).alias("n_channel_in_chat_web_phone"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 2)
)

sequenced = (
    totals
    .select(
        "*",
        F.ntile(10).over(w_n_channel_in_chat_web_phone).alias("cnt_decile"),
        F.dense_rank().over(w_cnt_distinct_agent_ids_by_cl).alias("closed_at_hour_to_csat_filled_ratio_dense_rank"),
    )
)

without_matches = (
    sequenced
    .crossJoin(sequenced.agg(F.max("closed_at_hour_to_csat_filled_ratio").alias("overall_max_closed_at_hour_to_csat_filled_ratio")))
    .filter(F.col("closed_at_hour_to_csat_filled_ratio") < F.col("overall_max_closed_at_hour_to_csat_filled_ratio"))
    .drop("overall_max_closed_at_hour_to_csat_filled_ratio")
)

result = (
    without_matches
    .select(
        "opened_at_month",
        "distinct_agent_ids",
        "closed_at_hour_to_csat_filled_ratio_dense_rank",
        "cnt",
    )
    .unpivot(
        ids=["opened_at_month"],
        values=["distinct_agent_ids", "closed_at_hour_to_csat_filled_ratio_dense_rank", "cnt"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
