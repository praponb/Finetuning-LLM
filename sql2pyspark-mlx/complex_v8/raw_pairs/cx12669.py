from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_requester_id = Window.orderBy("requester_id", "id")
w_all_by_requester_id_running = Window.orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_closed_at_year_closed_at_qua = Window.partitionBy("closed_at_year", "closed_at_quarter")
w_csat = Window.partitionBy("csat")

scoped = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        "tags",
        F.quarter(F.col("closed_at")).alias("closed_at_quarter"),
        (F.col("opened_at") + F.expr("INTERVAL 1 DAYS")).alias("opened_at_plus_1d"),
        F.year(F.col("closed_at")).alias("closed_at_year"),
    )
)

with_windows = (
    scoped
    .select(
        "*",
        F.ntile(10).over(w_all_by_requester_id).alias("requester_id_decile"),
        F.last("closed_at", ignorenulls=True).over(w_all_by_requester_id_running).alias("last_known_closed_at"),
        F.round(F.avg("requester_id").over(w_closed_at_year_closed_at_qua), 2).alias("closed_at_year_closed_at_quarter_avg_requester_i"),
    )
)

scored = (
    with_windows
    .filter(F.col("csat") > 2)
    .withColumn("tags_count", F.size(F.col("tags")))
)

scored_2 = (
    scored
    .select(
        "id",
        "requester_id",
        "closed_at",
        "csat",
        "tags",
        "closed_at_quarter",
        "opened_at_plus_1d",
        "closed_at_year",
        "last_known_closed_at",
        "closed_at_year_closed_at_quarter_avg_requester_i",
        "tags_count",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("last_known_closed_at")).alias("days_since_last_known_closed_at"),
        F.greatest(F.col("csat"), F.col("tags_count")).alias("greatest_csat_tags_count"),
    )
)

ranked = (
    scored_2
    .select(
        "*",
        F.round(F.col("closed_at_year") - F.avg("closed_at_year").over(w_csat), 2).alias("closed_at_year_vs_csat_avg"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_csat), F.lit(0)), 4).alias("csat_share_of_csat"),
    )
)

result = (
    ranked
    .select(
        "id",
        "closed_at",
        "csat",
        "tags",
        "closed_at_quarter",
        "opened_at_plus_1d",
        "closed_at_year",
        "closed_at_year_closed_at_quarter_avg_requester_i",
        "tags_count",
        "csat_share_of_csat",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_at_plus_1d")).alias("days_since_opened_at_plus_1d"),
        F.round((F.unix_timestamp(F.col("last_known_closed_at")) - F.unix_timestamp(F.col("opened_at_plus_1d"))) / 60, 1).alias("minutes_opened_at_plus_1d_to_last_known_closed_a"),
    )
    .orderBy(F.col("closed_at_year").desc())
)
