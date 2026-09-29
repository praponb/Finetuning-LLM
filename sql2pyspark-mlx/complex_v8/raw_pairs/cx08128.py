from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_total_ticket_count = Window.orderBy("total_ticket_count", "tag")
w_all_by_total_ticket_count_pr = Window.orderBy("total_ticket_count", "tag").rowsBetween(-3, -1)

valid = (
    df_tickets
    .filter(F.col("channel") == "email")
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "tags",
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
        F.concat(F.col("priority"), F.lit("#"), F.col("csat").cast("string")).alias("priority_ref"),
        F.date_trunc("week", F.col("closed_at")).alias("closed_at_week"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

summarized_rows = (
    valid.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

ticket_stats_2 = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("closed_at").alias("last_ticket_closed_at"),
        F.sum("csat").alias("total_ticket_csat"),
        F.min("opened_at").alias("first_ticket_opened_at"),
    )
)

summarized_rows_2 = (
    summarized_rows.alias("s")
    .join(ticket_stats_2.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.last_ticket_closed_at",
        "t.total_ticket_csat",
        "t.first_ticket_opened_at",
    )
)

exploded = (
    summarized_rows_2
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "requester_id_filled",
        "priority_ref",
        "closed_at_week",
        "ticket_count",
        "last_ticket_closed_at",
        "total_ticket_csat",
        "first_ticket_opened_at",
        F.explode_outer("tags").alias("tag"),
    )
)

rollup_base = (
    exploded
    .groupBy("tag")
    .agg(
        F.max("closed_at_week").alias("last_closed_at_week"),
        F.count("*").alias("row_count"),
        F.percentile_approx("requester_id_filled", 0.5).alias("median_requester_id_filled"),
        F.sum("ticket_count").alias("total_ticket_count"),
    )
)

scored = (
    rollup_base
    .select(
        "*",
        (F.floor(F.col("row_count") / 2) * 2).alias("row_count_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("tag")), " ", "_")).alias("tag_slug"),
    )
)

windowed = (
    scored
    .withColumn("seq_no", F.row_number().over(w_all_by_total_ticket_count))
    .withColumn("total_ticket_count_sum_3_prior", F.sum("total_ticket_count").over(w_all_by_total_ticket_count_pr))
)

result = (
    windowed
    .filter(F.col("total_ticket_count_sum_3_prior").between(29, 94))
    .select(
        "tag",
        "last_closed_at_week",
        "row_count",
        "median_requester_id_filled",
        "total_ticket_count",
        "row_count_bucket",
        "tag_slug",
        "seq_no",
        "total_ticket_count_sum_3_prior",
        F.date_trunc("hour", F.col("last_closed_at_week")).alias("last_closed_at_week_hour"),
        F.date_trunc("hour", F.col("last_closed_at_week")).alias("last_closed_at_week_hour_2"),
    )
    .orderBy(F.col("tag_slug").desc_nulls_first(), "row_count")
)
