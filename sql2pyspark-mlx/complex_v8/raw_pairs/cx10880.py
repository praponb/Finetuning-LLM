from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at_month = Window.partitionBy("event_type").orderBy("event_at_month", "id")
w_event_type_by_event_at_month_2 = Window.partitionBy("event_type").orderBy("event_at_month", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_event_at_month_3 = Window.partitionBy("event_type").orderBy("event_at_month")
w_event_type_prev2_event_at_to = Window.partitionBy("event_type", F.to_date("prev2_event_at")).orderBy("first_event_at_month")
w_first_event_at_month_month_e = Window.partitionBy(F.month("first_event_at_month"), "event_at_month_rank")
w_event_type_prev2_event_at_to_2 = Window.partitionBy("event_type", F.to_date("prev2_event_at")).orderBy("first_event_at_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_prev2_event_at_to_3 = Window.partitionBy("event_type", F.to_date("prev2_event_at")).orderBy("first_event_at_month", "id")

prepared = (
    df_ticket_events
    .filter(F.col("actor").isin("agent", "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
    )
)

windowed = (
    prepared
    .select(
        "id",
        "event_at",
        "event_type",
        "event_at_month",
        F.round((F.unix_timestamp("event_at_month") - F.unix_timestamp(F.lag("event_at_month").over(w_event_type_by_event_at_month))) / 60, 1).alias("minutes_since_prev_event_at_month"),
        F.first("event_at_month").over(w_event_type_by_event_at_month_2).alias("first_event_at_month"),
        F.round(F.percent_rank().over(w_event_type_by_event_at_month_3), 3).alias("event_at_month_percent_rank"),
        F.lag("event_at", 2).over(w_event_type_by_event_at_month).alias("prev2_event_at"),
        F.rank().over(w_event_type_by_event_at_month_3).alias("event_at_month_rank"),
    )
)

ranked = (
    windowed
    .select(
        "*",
        F.round(F.percent_rank().over(w_event_type_prev2_event_at_to), 3).alias("first_event_at_month_percent_rank"),
        F.round(F.col("event_at_month_percent_rank") / F.nullif(F.sum("event_at_month_percent_rank").over(w_first_event_at_month_month_e), F.lit(0)), 4).alias("event_at_month_percent_rank_share_of_first_event"),
        F.count(F.lit(1)).over(w_event_type_prev2_event_at_to_2).alias("running_count"),
        (F.col("event_at_month_rank") - F.lag("event_at_month_rank").over(w_event_type_prev2_event_at_to_3)).alias("event_at_month_rank_change"),
        F.count(F.lit(1)).over(w_first_event_at_month_month_e).alias("rows_in_first_event_at_month_month_event_at_mont"),
    )
)

result = (
    ranked
    .groupBy("event_at_month")
    .pivot("event_type", ["resolved", "assigned", "escalated", "replied"])
    .agg(F.count("event_at_month_rank_change"))
    .orderBy("event_at_month")
)
