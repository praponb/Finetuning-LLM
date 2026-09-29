from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy("event_at")
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_latest = Window.partitionBy("ticket_id", "event_type").orderBy("second_event_at", "id")
w_all_by_event_at_cume_dist_2 = Window.orderBy("event_at_cume_dist_2_adj", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_event_type = Window.partitionBy("actor", "event_type")
w_all_by_event_at_cume_dist_2_2 = Window.orderBy(F.col("event_at_cume_dist_2_minus_max").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_event_at_cume_dist_2_3 = Window.orderBy("event_at_cume_dist_2_adj")
w_all_by_event_at_cume_dist_2_4 = Window.orderBy(F.col("event_at_cume_dist_2_minus_max").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_ticket_events
    .filter((F.col("actor").isNotNull()) & (F.col("event_type").isin("created", "replied", "reopened")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.dense_rank().over(w_actor_by_event_at_ties).alias("event_at_dense_rank"),
        F.round(F.cume_dist().over(w_actor_by_event_at), 3).alias("event_at_cume_dist"),
        F.nth_value("event_at", 2).over(w_actor_by_event_at_full).alias("second_event_at"),
        F.round(F.cume_dist().over(w_actor_by_event_at_ties), 3).alias("event_at_cume_dist_2"),
    )
)

top_per_group = (
    with_windows
    .filter((F.col("event_at_dense_rank") == 1) & (F.col("event_at_cume_dist") >= 0.4))
    .drop("event_at_dense_rank")
)

scored = (
    top_per_group
    .filter(F.col("event_type").isin("reopened", "assigned"))
    .select(
        "*",
        (F.col("event_at_cume_dist_2") + F.col("event_at_cume_dist")).alias("event_at_cume_dist_2_plus_event_at_cume_dist"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
        F.round(F.col("event_at_cume_dist_2") * 1.07, 2).alias("event_at_cume_dist_2_adj"),
    )
)

ticket_id_stats = (
    scored
    .groupBy("ticket_id")
    .agg(F.max("event_at_cume_dist_2").alias("max_event_at_cume_dist_2"))
)

compared = (
    scored.alias("s")
    .join(ticket_id_stats.alias("g"), F.col("g.ticket_id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("s.event_at_cume_dist_2") == F.col("g.max_event_at_cume_dist_2"))
    .select(
        "s.*",
        "g.max_event_at_cume_dist_2",
        F.round(F.col("s.event_at_cume_dist_2") - F.col("g.max_event_at_cume_dist_2"), 2).alias("event_at_cume_dist_2_minus_max"),
    )
)

unique_rows = (
    compared
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

windowed = (
    unique_rows
    .select(
        "id",
        "event_at",
        "event_type",
        "event_type_slug",
        "event_at_cume_dist",
        "event_at_cume_dist_2_adj",
        "max_event_at_cume_dist_2",
        "event_at_cume_dist_2_minus_max",
        F.last("event_at", ignorenulls=True).over(w_all_by_event_at_cume_dist_2).alias("last_known_event_at"),
        F.size(F.collect_set("ticket_id").over(w_actor_event_type)).alias("distinct_ticket_id_in_actor_event_type"),
        F.max("event_at_cume_dist").over(w_all_by_event_at_cume_dist_2_2).alias("event_at_cume_dist_max_3"),
        F.round(F.percent_rank().over(w_all_by_event_at_cume_dist_2_3), 3).alias("event_at_cume_dist_2_adj_percent_rank"),
        F.max("event_at_cume_dist_2_adj").over(w_all_by_event_at_cume_dist_2_4).alias("max_event_at_cume_dist_2_adj_so_far"),
    )
)

result = (
    windowed
    .select(
        "id",
        "event_at",
        "event_type",
        "event_type_slug",
        "event_at_cume_dist",
        "event_at_cume_dist_2_adj",
        "max_event_at_cume_dist_2",
        "event_at_cume_dist_2_minus_max",
        "last_known_event_at",
        "event_at_cume_dist_max_3",
        F.dayofweek(F.col("last_known_event_at")).alias("last_known_event_at_dayofweek"),
    )
    .orderBy(
        F.col("last_known_event_at").desc_nulls_first(),
        F.col("event_at_cume_dist_max_3").desc_nulls_first(),
    )
)
