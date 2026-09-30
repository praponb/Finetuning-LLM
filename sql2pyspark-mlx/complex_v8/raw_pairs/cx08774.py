from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_group_actor_by_event_a = Window.partitionBy("actor_group", "actor").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_actor_group_event_type_by_ev = Window.partitionBy("actor_group", "event_type").orderBy("event_at", "id")
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id")
w_event_type_by_event_at_2 = Window.partitionBy("event_type").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_actor_group_actor_by_event_a_2 = Window.partitionBy("actor_group", "actor").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy(F.col("event_at").asc_nulls_last())
w_event_at_dense_rank_by_event = Window.partitionBy("event_at_dense_rank").orderBy("event_at_quartile_2", "id")
w_prev_event_at_month_event_at = Window.partitionBy(F.date_trunc("month", "prev_event_at"), "event_at_quintile")
w_prev_event_at_month_by_prev = Window.partitionBy(F.date_trunc("month", "prev_event_at")).orderBy(F.col("prev_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_at") >= F.lit("2025-01-25 07:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("actor").isin("system", "customer"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

with_trends = (
    filtered
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor_group",
        "event_type_actor_label",
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_group_actor_by_event_a))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.ntile(10).over(w_actor_group_event_type_by_ev).alias("event_at_decile"),
        F.lag("event_at").over(w_event_type_by_event_at).alias("prev_event_at"),
        F.ntile(5).over(w_actor_group_actor_by_event_a).alias("event_at_quintile"),
        F.ntile(4).over(w_event_type_by_event_at).alias("event_at_quartile"),
        F.lag("event_at").over(w_event_type_by_event_at_2).alias("prev_event_at_2"),
        F.ntile(5).over(w_all_by_event_at).alias("event_at_quintile_2"),
        F.nth_value("event_at", 2).over(w_actor_group_actor_by_event_a_2).alias("second_event_at"),
        F.round(F.cume_dist().over(w_event_type_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.ntile(4).over(w_actor_group_actor_by_event_a).alias("event_at_quartile_2"),
        F.row_number().over(w_all_by_event_at).alias("rn"),
        F.dense_rank().over(w_event_type_by_event_at_ties).alias("event_at_dense_rank"),
    )
)

annotated = (
    with_trends
    .filter(F.col("event_at_quintile_2") < 7)
    .select(
        "id",
        "event_type",
        "actor_group",
        "event_type_actor_label",
        "minutes_since_prev_event_at",
        "prev_event_at",
        "event_at_quintile",
        "event_at_quartile",
        "prev_event_at_2",
        "event_at_cume_dist",
        "event_at_quartile_2",
        "rn",
        "event_at_dense_rank",
        F.date_format(F.col("prev_event_at_2"), "MMM yyyy").alias("prev_event_at_2_label"),
    )
)

sequenced = (
    annotated
    .select(
        "*",
        F.ntile(3).over(w_event_at_dense_rank_by_event).alias("event_at_quartile_2_tercile"),
        F.round(F.avg("event_at_cume_dist").over(w_prev_event_at_month_event_at), 2).alias("prev_event_at_month_event_at_quintile_avg_event_"),
        F.round(F.col("event_at_quartile") - F.avg("event_at_quartile").over(w_prev_event_at_month_event_at), 2).alias("event_at_quartile_vs_prev_event_at_month_event_a"),
        F.min("minutes_since_prev_event_at").over(w_prev_event_at_month_event_at).alias("prev_event_at_month_event_at_quintile_min_minute"),
        F.nth_value("event_at_quintile", 2).over(w_prev_event_at_month_by_prev).alias("second_event_at_quintile"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "event_type",
        "event_type_actor_label",
        "prev_event_at",
        "event_at_quintile",
        "event_at_quartile",
        "prev_event_at_2",
        "event_at_quartile_2",
        "rn",
        "event_at_dense_rank",
        "prev_event_at_2_label",
        "prev_event_at_month_event_at_quintile_avg_event_",
        "event_at_quartile_vs_prev_event_at_month_event_a",
        "second_event_at_quintile",
        F.date_format(F.col("prev_event_at_2"), "MMM yyyy").alias("prev_event_at_2_label_2"),
    )
    .orderBy(
        "event_at_quartile_vs_prev_event_at_month_event_a",
        F.col("prev_event_at_2_label").asc_nulls_last(),
    )
)
