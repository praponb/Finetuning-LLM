from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id")
w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy("event_at")
w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_event_type_by_even_2 = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").asc_nulls_last())
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_by_event_at_2 = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_ticket_id_event_type_by_even_3 = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_all_by_event_at_dense_rank = Window.orderBy(F.col("event_at_dense_rank").desc(), "id")
w_next2_event_at_plus_1d_to_da = Window.partitionBy(F.to_date("next2_event_at_plus_1d"), "ticket_id").orderBy("event_at_tercile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor = Window.partitionBy("actor")

valid = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp")) & (F.col("event_type").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

metrics = (
    valid
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.round(F.percent_rank().over(w_event_type_actor_by_event_at), 3).alias("event_at_percent_rank"),
        F.dense_rank().over(w_actor_by_event_at_ties).alias("event_at_dense_rank"),
        F.first("event_at", ignorenulls=True).over(w_ticket_id_event_type_by_even).alias("first_known_event_at"),
        F.round(F.cume_dist().over(w_ticket_id_event_type_by_even_2), 3).alias("event_at_cume_dist"),
        F.rank().over(w_actor_by_event_at).alias("event_at_rank"),
        F.lag("event_at").over(w_actor_by_event_at_2).alias("prev_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.ntile(3).over(w_actor_by_event_at_2).alias("event_at_tercile"),
        F.ntile(4).over(w_all_by_event_at).alias("event_at_quartile"),
        F.lag("event_at").over(w_actor_by_event_at).alias("prev_event_at_2"),
        F.lag("event_at").over(w_ticket_id_event_type_by_even_3).alias("prev_event_at_3"),
        F.lead("event_at", 2).over(w_actor_by_event_at).alias("next2_event_at"),
        F.row_number().over(w_ticket_id_event_type_by_even_3).alias("rn"),
    )
)

annotated = (
    metrics
    .filter(F.col("event_at_tercile") < 8)
    .select(
        "*",
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        (F.col("next2_event_at") + F.expr("INTERVAL 1 DAYS")).alias("next2_event_at_plus_1d"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("rn").cast("string")).alias("event_type_ref"),
    )
)

with_trends = (
    annotated
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_at_percent_rank",
        "first_known_event_at",
        "event_at_cume_dist",
        "event_at_rank",
        "prev_event_at",
        "minutes_since_prev_event_at",
        "event_at_tercile",
        "event_at_quartile",
        "prev_event_at_2",
        "next2_event_at",
        "next2_event_at_plus_1d",
        "event_type_ref",
        F.round(F.percent_rank().over(w_all_by_event_at_dense_rank), 3).alias("event_at_dense_rank_percent_rank"),
        F.sum("event_at_percent_rank").over(w_next2_event_at_plus_1d_to_da).alias("running_event_at_percent_rank"),
        F.size(F.collect_set("ticket_id").over(w_actor)).alias("distinct_ticket_id_in_actor"),
    )
)

summary = (
    with_trends
    .groupBy("event_at_quartile")
    .agg(
        F.round(F.sum("event_at_tercile") / F.nullif(F.sum("event_at_percent_rank"), F.lit(0)), 3).alias("event_at_tercile_to_event_at_percent_rank_ratio"),
        F.sum(F.when(F.col("actor").isNotNull(), F.col("event_at_quartile")).otherwise(0)).alias("event_at_quartile_actor_is_not_null"),
        F.count("*").alias("row_count"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.sum("event_at_quartile").alias("total_event_at_quartile"),
        F.count(F.when(F.col("event_type") == "escalated", 1)).alias("n_event_type_escalated"),
    )
)

result = (
    summary
    .filter(F.col("total_event_at_quartile") > 13)
    .select(
        "event_at_quartile",
        "event_at_tercile_to_event_at_percent_rank_ratio",
        "event_at_quartile_actor_is_not_null",
        "row_count",
        "distinct_event_types",
        "total_event_at_quartile",
        "n_event_type_escalated",
        F.round(F.col("n_event_type_escalated") / F.nullif(F.col("row_count"), F.lit(0)), 3).alias("n_event_type_escalated_per_row_count"),
    )
    .orderBy(F.col("event_at_quartile").asc(), "event_at_quartile_actor_is_not_null")
)
