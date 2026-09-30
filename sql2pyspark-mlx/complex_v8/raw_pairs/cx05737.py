from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel")
w_opened_at_year_by_requester = Window.partitionBy(F.year("opened_at")).orderBy("requester_id", "id")

valid_tickets = (
    df_tickets
    .filter((F.col("channel").isin("chat", "web", "email")) & (F.col("opened_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.quarter(F.col("opened_at")).alias("opened_at_quarter"),
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
        F.quarter(F.col("closed_at")).alias("closed_at_quarter"),
    )
)

with_trends = (
    valid_tickets
    .select(
        "id",
        "requester_id",
        "opened_at",
        "channel",
        "csat",
        "tags",
        F.round(F.col("opened_at_quarter") / F.nullif(F.sum("opened_at_quarter").over(w_channel), F.lit(0)), 4).alias("opened_at_quarter_share_of_channel"),
        F.round(F.percent_rank().over(w_opened_at_year_by_requester), 3).alias("requester_id_percent_rank"),
        (F.col("closed_at_quarter") - F.lag("closed_at_quarter").over(w_opened_at_year_by_requester)).alias("closed_at_quarter_change"),
        F.dense_rank().over(w_opened_at_year_by_requester).alias("requester_id_dense_rank"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_opened_at_year_by_requester))) / 60, 1).alias("minutes_since_prev_opened_at"),
    )
)

top_per_group = (
    with_trends
    .filter(F.col("requester_id_dense_rank") <= 3)
    .drop("requester_id_dense_rank")
)

result = (
    top_per_group
    .select("id", "requester_id", "closed_at_quarter_change")
    .unpivot(
        ids=["id"],
        values=["requester_id", "closed_at_quarter_change"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric")
)
