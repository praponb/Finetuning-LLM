from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_id").orderBy("amount", "id")
w_grp_2 = Window.partitionBy(F.date_trunc("WEEK", "paid_at"), "order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("event_seq", "id")
w4 = Window.orderBy("cumulative_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter((F.col("paid_at") > F.to_date(F.lit("2023-11-12"))) & F.col("method").isNotNull())
    .select("*", F.trim("status").alias("clean_status"))
)

with_windows = (
    valid
    .select(
        "id",
        "clean_status",
        F.lead("amount").over(w_grp).alias("next_amount"),
        F.row_number().over(w_grp).alias("event_seq"),
        F.sum("amount").over(w_grp_2).alias("cumulative_amount"),
    )
)

with_windows_payments = (
    with_windows
    .filter(F.col("event_seq") < 4)
    .select(
        "id",
        "clean_status",
        "event_seq",
        "cumulative_amount",
        F.lead("event_seq").over(w3).alias("next_event_seq"),
        F.lag("next_amount").over(w3).alias("prev_next_amount"),
        F.lead("next_amount").over(w3).alias("next_next_amount"),
        F.row_number().over(w3).alias("row_seq"),
        F.last("next_amount", ignorenulls=True).over(w4).alias("last_known_next_amount"),
    )
)

valid_payments = (
    with_windows_payments
    .filter(F.col("cumulative_amount") < 4223)
    .select(
        "id",
        "clean_status",
        "event_seq",
        "cumulative_amount",
        "next_event_seq",
        "prev_next_amount",
        "next_next_amount",
        "row_seq",
        "last_known_next_amount",
        (F.col("event_seq") + F.col("row_seq")).alias("event_seq_plus_row_seq"),
    )
)

valid_payments_2 = (
    valid_payments
    .filter(F.col("cumulative_amount") != 5486)
    .select(
        "id",
        "next_event_seq",
        "prev_next_amount",
        "next_next_amount",
        "row_seq",
        F.floor("prev_next_amount").alias("prev_next_amount_whole"),
        F.greatest("cumulative_amount", "last_known_next_amount").alias("greatest_cumulative_amount_last_known_next_amount"),
    )
)

result = (
    valid_payments_2
    .groupBy(
        F.when(F.col("greatest_cumulative_amount_last_known_next_amount") >= 73, "HIGH").otherwise("LOW").alias("greatest_cumulative_amount_last_known_next_amount_tier"),
        F.when(F.col("row_seq") >= 9, "HIGH").otherwise("LOW").alias("row_seq_tier"),
        F.when(F.col("next_next_amount") >= 1590, "HIGH").otherwise("LOW").alias("next_next_amount_tier"),
    )
    .agg(
        F.min("prev_next_amount_whole").alias("min_prev_next_amount_whole"),
        F.percentile_approx("greatest_cumulative_amount_last_known_next_amount", 0.9).alias("greatest_cumulative_amount_last_known_next_amount_p90"),
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows_2"),
        F.percentile_approx("prev_next_amount_whole", [0.25, 0.5, 0.75]).alias("prev_next_amount_whole_quartiles"),
    )
    .orderBy(F.col("num_rows").desc())
)
