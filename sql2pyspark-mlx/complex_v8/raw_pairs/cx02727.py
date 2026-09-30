from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_opened_at = Window.orderBy("opened_at", "id", "tag_pos")
w_all_by_opened_at_running = Window.orderBy("opened_at", "id", "tag_pos").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_tickets = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "bug"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        F.size(F.col("tags")).alias("tags_count"),
    )
)

flattened = (
    base_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags_count",
        F.posexplode("tags").alias("tag_pos", "tag"),
    )
)

windowed = (
    flattened
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags_count",
        "tag_pos",
        (F.col("tag_pos") - F.lag("tag_pos").over(w_all_by_opened_at)).alias("tag_pos_change"),
        F.first("opened_at", ignorenulls=True).over(w_all_by_opened_at_running).alias("first_known_opened_at"),
    )
)

annotated = (
    windowed
    .select(
        "*",
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
        F.lower(F.trim(F.col("channel"))).alias("channel_clean"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

result = (
    annotated
    .groupingSets(
        [["tag_pos", "agent_id", "channel"], ["tag_pos"], []],
        "tag_pos", "agent_id", "channel",
    )
    .agg(
        F.sum(F.when(F.col("channel") == "phone", F.col("tag_pos_change")).otherwise(0)).alias("tag_pos_change_channel_phone"),
        F.percentile_approx("tag_pos_change", 0.5).alias("median_tag_pos_change"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
    )
    .select(
        "tag_pos",
        "agent_id",
        F.coalesce(F.col("channel"), F.lit("ALL")).alias("channel"),
        "tag_pos_change_channel_phone",
        "median_tag_pos_change",
        "distinct_agent_ids",
    )
)
