from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_month_by_event_at_f = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_month_by_event_at_r = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_event_type_actor_by_event_at_2 = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_rn_by_first_event_at_2 = Window.partitionBy("rn").orderBy("first_event_at_2", "id")
w_event_at_decile_first_event = Window.partitionBy("event_at_decile", F.date_trunc("month", "first_event_at")).orderBy("event_at", "id")
w_event_at_decile_by_rn_minus = Window.partitionBy("event_at_decile").orderBy("rn_minus_event_at_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_rn = Window.partitionBy("ticket_id", "rn")
w_next2_event_at_day_month = Window.partitionBy(F.month("next2_event_at_day"))
w_all_by_avg_rn = Window.orderBy(F.col("avg_rn").desc(), "next2_event_at_day")

recent = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp")) & (~F.col("actor").isin("agent", "customer")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
    )
)

ranked = (
    recent
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_type_ref",
        F.first("event_at").over(w_event_at_month_by_event_at_f).alias("first_event_at"),
        F.last("event_at", ignorenulls=True).over(w_event_at_month_by_event_at_r).alias("last_known_event_at"),
        F.last("event_at").over(w_ticket_id_by_event_at_full).alias("last_event_at"),
        F.lead("event_at", 2).over(w_event_type_actor_by_event_at).alias("next2_event_at"),
        F.nth_value("event_at", 2).over(w_ticket_id_by_event_at_full).alias("second_event_at"),
        F.row_number().over(w_event_type_actor_by_event_at).alias("rn"),
        F.ntile(10).over(w_ticket_id_by_event_at).alias("event_at_decile"),
        F.ntile(4).over(w_ticket_id_by_event_at).alias("event_at_quartile"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.lag("event_at").over(w_all_by_event_at).alias("prev_event_at"),
        F.first("event_at").over(w_ticket_id_by_event_at_full).alias("first_event_at_2"),
        F.rank().over(w_ticket_id_by_event_at).alias("event_at_rank"),
        F.nth_value("event_at", 2).over(w_event_type_actor_by_event_at_2).alias("second_event_at_2"),
        F.nth_value("event_at", 2).over(w_event_at_month_by_event_at_f).alias("second_event_at_3"),
    )
)

derived = (
    ranked
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "first_event_at",
        "next2_event_at",
        "second_event_at",
        "rn",
        "event_at_decile",
        "event_at_quartile",
        "minutes_since_prev_event_at",
        "first_event_at_2",
        "event_at_rank",
        "second_event_at_2",
        F.year(F.col("next2_event_at")).alias("next2_event_at_year"),
        (F.col("rn") - F.col("event_at_rank")).alias("rn_minus_event_at_rank"),
    )
)

ranked_2 = (
    derived
    .select(
        "*",
        F.round((F.unix_timestamp("second_event_at") - F.unix_timestamp(F.lag("second_event_at").over(w_rn_by_first_event_at_2))) / 60, 1).alias("minutes_since_prev_second_event_at"),
        F.row_number().over(w_event_at_decile_first_event).alias("rn_2"),
        F.sum("rn").over(w_event_at_decile_by_rn_minus).alias("running_rn"),
        F.size(F.collect_set("actor").over(w_ticket_id_rn)).alias("distinct_actor_in_ticket_id_rn"),
        F.max("event_at_quartile").over(w_ticket_id_rn).alias("ticket_id_rn_max_event_at_quartile"),
    )
)

summary = (
    ranked_2
    .groupBy(F.to_date("next2_event_at").alias("next2_event_at_day"))
    .agg(
        F.round(F.avg("rn"), 2).alias("avg_rn"),
        F.count("*").alias("row_count"),
        F.max("minutes_since_prev_second_event_at").alias("max_minutes_since_prev_second_event_at"),
        F.sum("minutes_since_prev_second_event_at").alias("total_minutes_since_prev_second_event_at"),
    )
    .filter(F.col("row_count") >= 2)
)

windowed = (
    summary
    .withColumn("row_count_vs_next2_event_at_day_month_avg", F.round(F.col("row_count") - F.avg("row_count").over(w_next2_event_at_day_month), 2))
    .withColumn("rn", F.row_number().over(w_all_by_avg_rn))
)

result = (
    windowed
    .select(
        "next2_event_at_day",
        "avg_rn",
        "row_count",
        "row_count_vs_next2_event_at_day_month_avg",
        "rn",
    )
    .orderBy(
        F.col("row_count_vs_next2_event_at_day_month_avg").desc_nulls_first(),
        "row_count",
    )
)
