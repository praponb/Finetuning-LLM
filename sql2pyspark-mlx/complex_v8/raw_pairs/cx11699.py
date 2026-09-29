from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_event_at_to_date = Window.partitionBy("event_type", F.to_date("event_at")).orderBy("event_at_week", "id")
w_all_by_event_at_date = Window.orderBy(F.col("event_at_date").desc(), "id")
w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_event_at_to_date = Window.partitionBy(F.to_date("event_at"))
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_event_type = Window.partitionBy("ticket_id", "event_type")
w_ticket_id_event_type_by_minu = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("minutes_since_prev_event_at_vs_event_at_to_date_").desc(), "id").rowsBetween(-6, -1)
w_actor_event_at_date_month_by = Window.partitionBy("actor", F.date_trunc("month", "event_at_date")).orderBy("minutes_since_prev_event_at_week", "id")
w_event_type_actor_by_event_at_2 = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(-2, -1)
w_minutes_since_prev_event_at = Window.partitionBy("minutes_since_prev_event_at_week_dense_rank").orderBy("event_at_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_by_prev3_minutes_since = Window.partitionBy("actor").orderBy("prev3_minutes_since_prev_event_at", "id")

base = (
    df_ticket_events
    .filter((F.col("ticket_id") <= 9) & (F.col("actor") == "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
)

windowed = (
    base
    .withColumn("minutes_since_prev_event_at_week", F.round((F.unix_timestamp("event_at_week") - F.unix_timestamp(F.lag("event_at_week").over(w_event_type_event_at_to_date))) / 60, 1))
    .withColumn("minutes_since_prev_event_at", F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at_date))) / 60, 1))
)

sequenced = (
    windowed
    .select(
        "*",
        F.datediff(F.col("event_at_date"), F.lag("event_at_date").over(w_actor_by_event_at)).alias("days_since_prev_event_at_date"),
        F.lag("minutes_since_prev_event_at", 3).over(w_actor_by_event_at).alias("prev3_minutes_since_prev_event_at"),
        F.round(F.col("minutes_since_prev_event_at") - F.avg("minutes_since_prev_event_at").over(w_event_at_to_date), 2).alias("minutes_since_prev_event_at_vs_event_at_to_date_"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

with_stats = (
    sequenced.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at")
)

windowed_2 = (
    with_stats
    .select(
        "*",
        F.row_number().over(w_event_type_actor_by_event_at).alias("event_at_seq"),
        F.count(F.lit(1)).over(w_ticket_id_event_type).alias("rows_in_ticket_id_event_type"),
        F.round(F.avg("minutes_since_prev_event_at").over(w_ticket_id_event_type_by_minu), 2).alias("minutes_since_prev_event_at_avg_6_prior"),
        F.dense_rank().over(w_actor_event_at_date_month_by).alias("minutes_since_prev_event_at_week_dense_rank"),
        F.sum("minutes_since_prev_event_at").over(w_event_type_actor_by_event_at_2).alias("minutes_since_prev_event_at_sum_2_prior"),
        F.round(F.col("minutes_since_prev_event_at") - F.avg("minutes_since_prev_event_at").over(w_ticket_id_event_type), 2).alias("minutes_since_prev_event_at_vs_ticket_id_event_t"),
    )
)

windowed_3 = (
    windowed_2
    .select(
        "*",
        F.sum("event_at_seq").over(w_minutes_since_prev_event_at).alias("running_event_at_seq"),
        F.round(F.cume_dist().over(w_actor_by_prev3_minutes_since), 3).alias("prev3_minutes_since_prev_event_at_cume_dist"),
    )
)

result = (
    windowed_3
    .select(
        "id",
        "event_type",
        "event_at_date",
        "event_at_week",
        "minutes_since_prev_event_at_week",
        "minutes_since_prev_event_at",
        "days_since_prev_event_at_date",
        "prev3_minutes_since_prev_event_at",
        "minutes_since_prev_event_at_vs_event_at_to_date_",
        "last_event_at",
        "event_at_seq",
        "minutes_since_prev_event_at_avg_6_prior",
        "minutes_since_prev_event_at_week_dense_rank",
        "minutes_since_prev_event_at_vs_ticket_id_event_t",
        "running_event_at_seq",
        "prev3_minutes_since_prev_event_at_cume_dist",
    )
    .orderBy(F.col("event_at_seq").desc())
)
