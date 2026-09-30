from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy("event_at", "id")
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type = Window.partitionBy("event_type")
w_event_type_actor_by_event_at_2 = Window.partitionBy("event_type", "actor").orderBy("event_at", "id")
w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id")
w_event_type_actor_by_event_at_3 = Window.partitionBy("event_type", "actor").orderBy("event_at", "id").rowsBetween(-6, -1)
w_actor_event_type_by_event_at_2 = Window.partitionBy("actor", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_ticket_events
    .filter((F.col("actor") != "customer") & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

ranked = (
    prepared
    .select(
        "*",
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_event_type_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.lag("event_at").over(w_actor_event_type_by_event_at).alias("prev_event_at"),
        F.last("event_at", ignorenulls=True).over(w_event_type_actor_by_event_at).alias("last_known_event_at"),
        F.count(F.lit(1)).over(w_event_type).alias("rows_in_event_type"),
        F.round(F.percent_rank().over(w_event_type_actor_by_event_at_2), 3).alias("event_at_percent_rank"),
        F.row_number().over(w_event_type_actor_by_event_at_2).alias("rn"),
        F.round(F.col("days_since_event_at") / F.nullif(F.sum("days_since_event_at").over(w_event_type), F.lit(0)), 4).alias("days_since_event_at_share_of_event_type"),
        F.lead("event_at").over(w_actor_ticket_id_by_event_at).alias("next_event_at"),
        F.sum("days_since_event_at").over(w_event_type_actor_by_event_at_3).alias("days_since_event_at_sum_6_prior"),
        F.nth_value("event_at", 2).over(w_actor_event_type_by_event_at_2).alias("second_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_ticket_id_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at_2"),
    )
)

top_rows = (
    ranked
    .filter((F.col("rn") <= 2) & (F.col("rows_in_event_type") < 18))
    .drop("rn")
)

result = (
    top_rows
    .filter(F.col("event_at_percent_rank") <= 0.7)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "days_since_event_at",
        "minutes_since_prev_event_at",
        "prev_event_at",
        "rows_in_event_type",
        "event_at_percent_rank",
        "days_since_event_at_share_of_event_type",
        "next_event_at",
        "days_since_event_at_sum_6_prior",
        "minutes_since_prev_event_at_2",
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
        F.when(F.col("event_type").isin("assigned", "resolved"), "group_a").when(F.col("event_type") == "reopened", "group_b").otherwise("other").alias("event_type_group"),
    )
    .orderBy(F.col("rows_in_event_type").asc(), "id")
    .limit(10)
)
