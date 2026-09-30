from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy("event_at", "id")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_all_by_event_at = Window.orderBy("event_at", "id")
w_all_by_event_at_2 = Window.orderBy(F.col("event_at").desc(), "id")
w_all_by_row_count_moving_2 = Window.orderBy("row_count", "actor", "next_event_at_2_month", "event_at_ym").rowsBetween(-2, Window.currentRow)
w_actor_row_count = Window.partitionBy("actor", "row_count")

cleaned_events = (
    df_ticket_events
    .filter((F.col("actor").isin("agent", "system")) | (F.col("ticket_id") >= 14))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_windows = (
    cleaned_events
    .select(
        "*",
        F.lead("event_at").over(w_event_type_ticket_id_by_even).alias("next_event_at"),
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
        F.lag("event_at", 3).over(w_event_type_ticket_id_by_even).alias("prev3_event_at"),
        F.row_number().over(w_all_by_event_at).alias("rn"),
        F.row_number().over(w_all_by_event_at_2).alias("seq_no"),
        F.lead("event_at").over(w_all_by_event_at_2).alias("next_event_at_2"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.ntile(10).over(w_all_by_event_at_2).alias("event_at_decile"),
        F.round(F.percent_rank().over(w_all_by_event_at_ties), 3).alias("event_at_percent_rank"),
    )
)

first_rows = (
    with_windows
    .filter((F.col("event_at_rank") <= 2) & (F.col("event_at") >= F.lit("2025-01-19 11:00:00").cast("timestamp")))
)

rollup_base = (
    first_rows
    .groupBy(
        "actor",
        F.date_trunc("month", "next_event_at_2").alias("next_event_at_2_month"),
        F.date_format("event_at", "yyyy-MM").alias("event_at_ym"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.sum("seq_no") / F.nullif(F.sum("event_at_rank"), F.lit(0)), 3).alias("seq_no_to_event_at_rank_ratio"),
        F.avg("rn").alias("avg_rn_check"),
    )
    .filter(F.col("avg_rn_check") > 2)
    .drop("avg_rn_check")
)

metrics = (
    rollup_base
    .withColumn("row_count_avg_3", F.round(F.avg("row_count").over(w_all_by_row_count_moving_2), 2))
    .withColumn("rows_in_actor_row_count", F.count(F.lit(1)).over(w_actor_row_count))
)

result = (
    metrics
    .select(
        "actor",
        "event_at_ym",
        "row_count",
        "seq_no_to_event_at_rank_ratio",
        "row_count_avg_3",
        "rows_in_actor_row_count",
        F.date_format(F.col("next_event_at_2_month"), "yyyy-MM-dd HH:00").alias("next_event_at_2_month_label"),
    )
    .orderBy(
        "row_count_avg_3",
        F.col("next_event_at_2_month_label").asc_nulls_last(),
    )
)
