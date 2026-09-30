from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_opened_at_full = Window.orderBy(F.col("opened_at").desc_nulls_first(), "id", "tag_pos").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_tag_pos_by_closed_a = Window.partitionBy("priority", "tag_pos").orderBy(F.col("closed_at").desc_nulls_first(), "id", "tag_pos").rowsBetween(-6, Window.currentRow)
w_all_by_opened_at_moving_6 = Window.orderBy(F.col("opened_at").desc_nulls_first(), "id", "tag_pos").rowsBetween(-6, Window.currentRow)
w_priority_tag_pos = Window.partitionBy("priority", "tag_pos")
w_all_by_requester_id_full = Window.orderBy(F.col("requester_id").asc_nulls_last(), "id", "tag_pos").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_median_rows_in_priori = Window.orderBy("median_rows_in_priority_tag_pos")

cleaned_tickets = (
    df_tickets
    .filter(F.col("csat") >= 3)
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
    )
)

flattened = (
    cleaned_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.posexplode("tags").alias("tag_pos", "tag"),
    )
)

sequenced = (
    flattened
    .select(
        "id",
        "requester_id",
        "closed_at",
        "priority",
        "csat",
        "tag_pos",
        "tag",
        F.first("requester_id").over(w_all_by_opened_at_full).alias("first_requester_id"),
        F.round(F.avg("tag_pos").over(w_priority_tag_pos_by_closed_a), 2).alias("tag_pos_avg_7"),
        F.max("requester_id").over(w_all_by_opened_at_moving_6).alias("requester_id_max_7"),
        F.count(F.lit(1)).over(w_priority_tag_pos).alias("rows_in_priority_tag_pos"),
    )
)

priority_stats = (
    sequenced
    .groupBy("priority")
    .agg(F.percentile_approx("rows_in_priority_tag_pos", 0.5).alias("median_rows_in_priority_tag_pos"))
)

vs_group = (
    sequenced.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.rows_in_priority_tag_pos") >= F.col("g.median_rows_in_priority_tag_pos"))
    .select(
        "s.*",
        "g.median_rows_in_priority_tag_pos",
        F.round(F.col("s.rows_in_priority_tag_pos") - F.col("g.median_rows_in_priority_tag_pos"), 2).alias("rows_in_priority_tag_pos_minus_median"),
    )
)

ranked = (
    vs_group
    .select(
        "*",
        F.nth_value("requester_id_max_7", 2).over(w_all_by_requester_id_full).alias("second_requester_id_max_7"),
        F.round(F.cume_dist().over(w_all_by_median_rows_in_priori), 3).alias("median_rows_in_priority_tag_pos_cume_dist"),
    )
)

priority_stats_2 = (
    ranked
    .groupBy("priority")
    .agg(F.avg("rows_in_priority_tag_pos").alias("avg_rows_in_priority_tag_pos"))
)

outliers = (
    ranked.alias("s")
    .join(priority_stats_2.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.rows_in_priority_tag_pos") > F.col("g.avg_rows_in_priority_tag_pos"))
    .select(
        "s.*",
        "g.avg_rows_in_priority_tag_pos",
        F.round(F.col("s.rows_in_priority_tag_pos") - F.col("g.avg_rows_in_priority_tag_pos"), 2).alias("rows_in_priority_tag_pos_minus_avg"),
    )
)

result = (
    outliers
    .select(
        "id",
        "requester_id",
        "closed_at",
        "priority",
        "tag_pos",
        "tag",
        "tag_pos_avg_7",
        "requester_id_max_7",
        "rows_in_priority_tag_pos",
        "median_rows_in_priority_tag_pos",
        "rows_in_priority_tag_pos_minus_median",
        "second_requester_id_max_7",
        "median_rows_in_priority_tag_pos_cume_dist",
        "avg_rows_in_priority_tag_pos",
        "rows_in_priority_tag_pos_minus_avg",
    )
)
