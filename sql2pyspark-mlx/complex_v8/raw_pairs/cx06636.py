from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_closed_at_quarter_run = Window.orderBy("closed_at_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_month = Window.partitionBy(F.date_trunc("month", "opened_at"))
w_all_by_closed_at_quarter_ful = Window.orderBy("closed_at_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_tickets = (
    df_tickets
    .filter((F.col("closed_at") >= F.lit("2025-01-16 00:00:00").cast("timestamp")) & (F.col("csat").between(2, 4)))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.date_trunc("quarter", F.col("closed_at")).alias("closed_at_quarter"),
    )
)

with_windows = (
    cleaned_tickets
    .select(
        "id",
        "requester_id",
        "opened_at",
        "priority",
        "channel",
        "tags",
        F.sum("requester_id").over(w_all_by_closed_at_quarter_run).alias("running_requester_id"),
        F.max("requester_id").over(w_opened_at_month).alias("opened_at_month_max_requester_id"),
        F.first("requester_id").over(w_all_by_closed_at_quarter_ful).alias("first_requester_id"),
    )
)

derived = (
    with_windows
    .filter(F.col("opened_at") >= F.lit("2025-01-19 07:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "opened_at",
        "channel",
        "tags",
        "first_requester_id",
        (F.floor(F.col("first_requester_id") / 5) * 5).alias("first_requester_id_bucket"),
    )
)

result = (
    derived
    .select(
        "id",
        "requester_id",
        "opened_at",
        "channel",
        "tags",
        "first_requester_id",
        "first_requester_id_bucket",
        F.date_format(F.col("opened_at"), "yyyy-MM-dd HH:00").alias("opened_at_label"),
    )
)
