from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_closed_at_to_date_by = Window.partitionBy("channel", F.to_date("closed_at")).orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_requester_id_running = Window.orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_quarter_agent_id_b = Window.partitionBy("opened_at_quarter", "agent_id").orderBy(F.col("closed_at").desc(), "id")
w_priority_opened_at_quarter = Window.partitionBy("priority", "opened_at_quarter")

base_tickets = (
    df_tickets
    .filter((F.col("opened_at") < F.lit("2025-02-24 08:00:00").cast("timestamp")) & (F.col("agent_id").isNotNull()))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
    )
)

tagged = (
    base_tickets
    .filter(F.col("channel") == "chat")
    .select("*", F.quarter(F.col("opened_at")).alias("opened_at_quarter"))
)

sequenced = (
    tagged
    .select(
        "id",
        "requester_id",
        "channel",
        "tags",
        "opened_at_quarter",
        F.max("opened_at_quarter").over(w_channel_closed_at_to_date_by).alias("max_opened_at_quarter_so_far"),
        F.first("opened_at_quarter", ignorenulls=True).over(w_channel_closed_at_to_date_by).alias("first_known_opened_at_quarter"),
        F.count(F.lit(1)).over(w_all_by_requester_id_running).alias("running_count"),
        (F.col("requester_id") - F.lag("requester_id").over(w_opened_at_quarter_agent_id_b)).alias("requester_id_change"),
        F.min("requester_id").over(w_priority_opened_at_quarter).alias("priority_opened_at_quarter_min_requester_id"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "requester_id",
        "channel",
        "tags",
        "opened_at_quarter",
        "max_opened_at_quarter_so_far",
        "first_known_opened_at_quarter",
        "requester_id_change",
        "priority_opened_at_quarter_min_requester_id",
        (F.col("running_count") - F.col("requester_id")).alias("running_count_minus_requester_id"),
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
    )
    .orderBy(
        F.col("id").desc(),
        F.col("first_known_opened_at_quarter").desc_nulls_first(),
    )
)
