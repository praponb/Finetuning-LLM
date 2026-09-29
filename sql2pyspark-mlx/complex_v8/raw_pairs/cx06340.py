from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_quarter_tier_opene = Window.partitionBy("closed_at_quarter_tier", F.date_trunc("month", "opened_at"))
w_all_by_hours_opened_at_to_cl = Window.orderBy("hours_opened_at_to_closed_at", "id")
w_all_by_minutes_opened_at_to = Window.orderBy("minutes_opened_at_to_closed_at")
w_opened_at_month_closed_at_qu = Window.partitionBy(F.month("opened_at"), "closed_at_quarter_tier").orderBy(F.col("minutes_opened_at_to_closed_at").asc_nulls_last(), "id")

valid = (
    df_tickets
    .filter((F.col("opened_at") < F.lit("2025-03-02 10:00:00").cast("timestamp")) | (F.col("channel") == "web"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        F.quarter(F.col("closed_at")).alias("closed_at_quarter"),
    )
)

annotated = (
    valid
    .select(
        "*",
        F.substring(F.trim(F.col("channel")), 1, 4).alias("channel_prefix"),
    )
)

labeled = (
    annotated
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
        F.date_format(F.col("closed_at"), "yyyy-MM-dd").alias("closed_at_label"),
        F.when(F.col("closed_at_quarter") >= 3, "large").when(F.col("closed_at_quarter") >= 2, "medium").otherwise("small").alias("closed_at_quarter_tier"),
    )
)

enriched = (
    labeled
    .filter(F.col("opened_at") < F.lit("2025-03-02 10:00:00").cast("timestamp"))
    .select(
        "id",
        "opened_at",
        "closed_at",
        "channel_prefix",
        "minutes_opened_at_to_closed_at",
        "closed_at_quarter_tier",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600, 1).alias("hours_opened_at_to_closed_at"),
        F.substring(F.trim(F.col("channel_prefix")), 1, 3).alias("channel_prefix_prefix"),
    )
)

windowed = (
    enriched
    .select(
        "*",
        F.sum("hours_opened_at_to_closed_at").over(w_closed_at_quarter_tier_opene).alias("closed_at_quarter_tier_opened_at_month_sum_hours"),
        F.row_number().over(w_all_by_hours_opened_at_to_cl).alias("hours_opened_at_to_closed_at_seq"),
        F.round(F.percent_rank().over(w_all_by_minutes_opened_at_to), 3).alias("minutes_opened_at_to_closed_at_percent_rank"),
        F.row_number().over(w_opened_at_month_closed_at_qu).alias("rn"),
    )
)

result = (
    windowed
    .filter(F.col("minutes_opened_at_to_closed_at_percent_rank").between(0.1, 0.9))
    .select(
        "id",
        "opened_at",
        "closed_at",
        "channel_prefix",
        "closed_at_quarter_tier",
        "hours_opened_at_to_closed_at",
        "closed_at_quarter_tier_opened_at_month_sum_hours",
        "hours_opened_at_to_closed_at_seq",
        "minutes_opened_at_to_closed_at_percent_rank",
        "rn",
    )
    .orderBy(F.col("hours_opened_at_to_closed_at_seq").desc_nulls_first())
)
