from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy("event_at", "id")
w_event_type_ticket_id_by_even_2 = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc_nulls_first(), "id")
w_actor_event_type_by_event_at_2 = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_ticket_id_by_even_3 = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_ticket_id_by_even_4 = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").desc())
w_all_by_event_at_percent_rank = Window.orderBy(F.col("event_at_percent_rank").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_event_at_terci = Window.partitionBy("event_type").orderBy("event_at_tercile", "id")
w_all_by_event_at_percent_rank_2 = Window.orderBy(F.col("event_at_percent_rank").asc_nulls_last(), "id")
w_ticket_id_event_at_year = Window.partitionBy("ticket_id", F.year("event_at"))
w_first_known_event_at_to_date = Window.partitionBy(F.to_date("first_known_event_at"), "event_at_tercile").orderBy(F.col("event_at_percent_rank").desc())
w_all_by_first_known_event_at = Window.orderBy(F.col("first_known_event_at").desc())
w_first_known_event_at_to_date_2 = Window.partitionBy(F.to_date("first_known_event_at"), "event_at_tercile").orderBy(F.col("event_at_percent_rank").desc(), "id")
w_min_first_known_event_at_den = Window.partitionBy("min_first_known_event_at_dense_rank", F.to_date("first_known_event_at_month"))
w_min_first_known_event_at_den_2 = Window.partitionBy("min_first_known_event_at_dense_rank", "event_at_year").orderBy("cnt", "event_at_year", "first_known_event_at_month")

recent_events = (
    df_ticket_events
    .filter((~F.col("event_type").isin("reopened", "escalated")) & (~F.col("actor").isin("system", "agent")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

windowed = (
    recent_events
    .select(
        "*",
        F.lag("event_at").over(w_event_type_ticket_id_by_even).alias("prev_event_at"),
        F.ntile(3).over(w_event_type_ticket_id_by_even_2).alias("event_at_tercile"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_event_type_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.last("event_at").over(w_actor_event_type_by_event_at_2).alias("last_event_at"),
        F.first("event_at", ignorenulls=True).over(w_event_type_ticket_id_by_even_3).alias("first_known_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_ticket_id_by_even))) / 60, 1).alias("minutes_since_prev_event_at_2"),
        F.dense_rank().over(w_event_type_ticket_id_by_even_4).alias("event_at_dense_rank"),
        F.round(F.percent_rank().over(w_event_type_ticket_id_by_even_4), 3).alias("event_at_percent_rank"),
    )
)

leaders = (
    windowed
    .filter(F.col("event_at_dense_rank") == 1)
    .drop("event_at_dense_rank")
)

with_windows = (
    leaders
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "prev_event_at",
        "event_at_tercile",
        "minutes_since_prev_event_at",
        "last_event_at",
        "first_known_event_at",
        "minutes_since_prev_event_at_2",
        "event_at_percent_rank",
        F.sum("minutes_since_prev_event_at").over(w_all_by_event_at_percent_rank).alias("running_minutes_since_prev_event_at"),
        F.row_number().over(w_event_type_by_event_at_terci).alias("seq_no"),
        F.row_number().over(w_all_by_event_at_percent_rank_2).alias("seq_no_2"),
        F.count(F.lit(1)).over(w_ticket_id_event_at_year).alias("rows_in_ticket_id_event_at_year"),
        F.dense_rank().over(w_first_known_event_at_to_date).alias("event_at_percent_rank_dense_rank"),
        F.dense_rank().over(w_all_by_first_known_event_at).alias("first_known_event_at_dense_rank"),
        F.row_number().over(w_first_known_event_at_to_date_2).alias("seq_no_4"),
    )
)

totals = (
    with_windows
    .groupBy(
        F.year("event_at").alias("event_at_year"),
        F.date_trunc("month", "first_known_event_at").alias("first_known_event_at_month"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.percentile_approx("event_at_tercile", 0.5).alias("median_event_at_tercile"),
        F.min("first_known_event_at_dense_rank").alias("min_first_known_event_at_dense_rank"),
    )
    .filter(F.col("cnt") >= 2)
)

metrics = (
    totals
    .withColumn("distinct_ids_share_of_min_first_known_event_at_d", F.round(F.col("distinct_ids") / F.nullif(F.sum("distinct_ids").over(w_min_first_known_event_at_den), F.lit(0)), 4))
    .withColumn("event_at_year_change", F.col("event_at_year") - F.lag("event_at_year").over(w_min_first_known_event_at_den_2))
)

result = (
    metrics
    .select(
        "event_at_year",
        "first_known_event_at_month",
        "distinct_ids",
        "cnt",
        "min_first_known_event_at_dense_rank",
        F.abs(F.col("median_event_at_tercile") - 4).alias("median_event_at_tercile_distance"),
        F.coalesce(F.col("cnt"), F.lit(0)).alias("cnt_filled"),
    )
    .orderBy(F.col("event_at_year").asc())
)
