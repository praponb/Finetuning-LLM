from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_closed_at = Window.orderBy("closed_at", "id")
w_all_by_opened_at_ties = Window.orderBy(F.col("opened_at").desc())
w_priority_closed_at_month_by = Window.partitionBy("priority", F.month("closed_at")).orderBy(F.col("opened_at").desc_nulls_first())
w_all_by_opened_at = Window.orderBy(F.col("opened_at").desc(), "id")
w_all_by_closed_at_running = Window.orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_closed_at_ties = Window.orderBy("closed_at")

cleaned = (
    df_tickets
    .filter(F.hour(F.col("opened_at")).between(9, 11))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "tags",
    )
)

with_trends = (
    cleaned
    .select(
        "*",
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_all_by_closed_at))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.round(F.cume_dist().over(w_all_by_opened_at_ties), 3).alias("opened_at_cume_dist"),
        F.round(F.cume_dist().over(w_priority_closed_at_month_by), 3).alias("opened_at_cume_dist_2"),
        F.rank().over(w_all_by_closed_at).alias("closed_at_rank"),
        F.lead("opened_at", 2).over(w_all_by_opened_at).alias("next2_opened_at"),
        F.row_number().over(w_all_by_closed_at).alias("closed_at_seq"),
        F.lag("closed_at", 3).over(w_all_by_closed_at).alias("prev3_closed_at"),
        F.last("opened_at", ignorenulls=True).over(w_all_by_closed_at_running).alias("last_known_opened_at"),
        F.round(F.percent_rank().over(w_all_by_closed_at_ties), 3).alias("closed_at_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_closed_at), 3).alias("closed_at_percent_rank_2"),
        F.lead("opened_at").over(w_all_by_closed_at).alias("next_opened_at"),
        F.dense_rank().over(w_priority_closed_at_month_by).alias("opened_at_dense_rank"),
    )
)

top_per_group = (
    with_trends
    .filter((F.col("closed_at_rank") <= 3) & (F.col("closed_at_seq") <= 7))
    .drop("closed_at_rank")
)

result = (
    top_per_group
    .select(
        "id",
        "opened_at",
        "closed_at",
        "tags",
        "minutes_since_prev_closed_at",
        "opened_at_cume_dist",
        "next2_opened_at",
        "closed_at_seq",
        "prev3_closed_at",
        "last_known_opened_at",
        "closed_at_percent_rank",
        "closed_at_percent_rank_2",
        "opened_at_dense_rank",
        F.upper(F.trim(F.col("priority"))).alias("priority_clean"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("last_known_opened_at")).alias("days_since_last_known_opened_at"),
    )
    .orderBy(F.col("closed_at").desc(), "id")
    .limit(5)
)
