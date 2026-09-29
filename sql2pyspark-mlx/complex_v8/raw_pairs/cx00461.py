from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_month_by_event_at = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy("event_at", "id")
w_event_at_month_by_event_at_t = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy("event_at")
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id")
w_event_at_month_by_event_at_f = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_group_by_event_at = Window.partitionBy("event_type_group").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_group_by_event_at_2 = Window.partitionBy("event_type_group").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_event_at_month_by_event_at_2 = Window.partitionBy(F.month("event_at")).orderBy(F.col("event_at").asc_nulls_last(), "id")
w_event_type_by_event_at_runni = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_tercile_by_first_ev = Window.partitionBy("event_at_tercile").orderBy(F.col("first_event_at").desc(), "id")
w_event_at_tercile_by_first_ev_2 = Window.partitionBy("event_at_tercile").orderBy(F.col("first_event_at").desc(), "id").rowsBetween(-1, Window.currentRow)
w_event_at_dense_rank = Window.partitionBy("event_at_dense_rank")
w_event_at_rank_event_at_month = Window.partitionBy("event_at_rank", F.date_trunc("month", "event_at"))
w_event_at_tercile_by_first_ev_3 = Window.partitionBy("event_at_tercile").orderBy(F.col("first_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_events = (
    df_ticket_events
    .filter(F.col("event_type") == "escalated")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("created", "assigned"), "group_a").when(F.col("event_type") == "escalated", "group_b").otherwise("other").alias("event_type_group"),
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

sequenced = (
    cleaned_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_type_clean",
        F.row_number().over(w_event_at_month_by_event_at).alias("rn"),
        F.rank().over(w_event_at_month_by_event_at_t).alias("event_at_rank"),
        F.ntile(3).over(w_event_type_by_event_at).alias("event_at_tercile"),
        F.first("event_at").over(w_event_at_month_by_event_at_f).alias("first_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_at_month_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.lead("event_at").over(w_event_type_by_event_at).alias("next_event_at"),
        F.last("event_at").over(w_event_at_month_by_event_at_f).alias("last_event_at"),
        F.first("event_at", ignorenulls=True).over(w_event_type_group_by_event_at).alias("first_known_event_at"),
        F.lag("event_at").over(w_event_type_group_by_event_at_2).alias("prev_event_at"),
        F.dense_rank().over(w_event_at_month_by_event_at_2).alias("event_at_dense_rank"),
        F.first("event_at", ignorenulls=True).over(w_event_type_by_event_at_runni).alias("first_known_event_at_2"),
    )
)

first_rows = sequenced.filter(F.col("rn") == 1)

with_trends = (
    first_rows
    .select(
        "*",
        F.lead("first_known_event_at_2", 3).over(w_event_at_tercile_by_first_ev).alias("next3_first_known_event_at_2"),
        F.sum("event_at_tercile").over(w_event_at_tercile_by_first_ev_2).alias("event_at_tercile_sum_2"),
        F.size(F.collect_set("event_type").over(w_event_at_dense_rank)).alias("distinct_event_type_in_event_at_dense_rank"),
        F.sum("rn").over(w_event_at_rank_event_at_month).alias("event_at_rank_event_at_month_sum_rn"),
        F.max("event_at_rank").over(w_event_at_tercile_by_first_ev_3).alias("max_event_at_rank_so_far"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type_clean",
        "rn",
        "event_at_rank",
        "first_event_at",
        "minutes_since_prev_event_at",
        "next_event_at",
        "last_event_at",
        "prev_event_at",
        "event_at_dense_rank",
        "first_known_event_at_2",
        "next3_first_known_event_at_2",
        "event_at_tercile_sum_2",
        "max_event_at_rank_so_far",
    )
    .orderBy(
        F.col("event_at_tercile_sum_2").desc(),
        F.col("minutes_since_prev_event_at").asc(),
        "id",
    )
    .limit(5)
)
