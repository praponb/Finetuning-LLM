from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("order_id").orderBy(F.col("order_amount").desc())
w3 = Window.partitionBy(F.month("order_date"), "order_id").orderBy("amount", "id")
w4 = Window.orderBy(F.col("order_date").desc(), "id")
w5 = Window.partitionBy(F.date_trunc("WEEK", "order_date"), "order_id")
w6 = Window.partitionBy("order_id").orderBy("prev_order_amount", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("order_id").orderBy("last4_sum_row_seq", "id")
w_grp = Window.partitionBy("order_id").orderBy("last4_max_row_seq", "id")
w9 = Window.partitionBy("order_id").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("order_id").orderBy(F.col("next_last4_min_row_seq").asc_nulls_first())
w11 = Window.partitionBy("order_id").orderBy(F.col("next_last4_max_row_seq").asc_nulls_first())
w_part = Window.partitionBy("order_id").orderBy("last4_min_row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.orderBy("last4_min_row_seq", "id").rowsBetween(-2, Window.currentRow)

enriched = (
    df_payments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.shipping_fee") <= 32)
    .select(
        "e.id",
        "e.order_id",
        "e.amount",
        F.col("p.amount").alias("order_amount"),
        F.col("p.order_date"),
    )
)

with_windows = (
    enriched
    .withColumn("amount_avg_6_prior", F.avg("amount").over(w))
    .withColumn("order_amount_percent_rank", F.percent_rank().over(w2))
    .withColumn("prev_order_amount", F.lag("order_amount", 1).over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("order_date_period_sum_amount", F.sum("amount").over(w5))
)

enriched_payments = (
    with_windows
    .select(
        "id",
        "order_id",
        F.sum("row_seq").over(w6).alias("last4_sum_row_seq"),
        F.min("row_seq").over(w6).alias("last4_min_row_seq"),
        F.max("row_seq").over(w6).alias("last4_max_row_seq"),
    )
)

enriched_payments_2 = (
    enriched_payments
    .withColumn("prev_last4_max_row_seq", F.lag("last4_max_row_seq").over(w7))
    .withColumn("next_last4_max_row_seq", F.lead("last4_max_row_seq").over(w7))
    .withColumn("next_last4_min_row_seq", F.lead("last4_min_row_seq").over(w7))
    .withColumn("event_seq", F.row_number().over(w7))
)

with_windows_payments = (
    enriched_payments_2
    .select(
        "id",
        "last4_min_row_seq",
        "last4_max_row_seq",
        "next_last4_max_row_seq",
        "event_seq",
        F.lag("last4_min_row_seq", 1).over(w_grp).alias("prev_last4_min_row_seq"),
        F.first("last4_max_row_seq", ignorenulls=True).over(w9).alias("first_known_last4_max_row_seq"),
        F.percent_rank().over(w_hist).alias("next_last4_min_row_seq_percent_rank"),
        F.percent_rank().over(w11).alias("next_last4_max_row_seq_percent_rank"),
        F.first("last4_max_row_seq", ignorenulls=True).over(w_part).alias("first_known_last4_max_row_seq_2"),
    )
)

scored = (
    with_windows_payments
    .select(
        "*",
        F.count("event_seq").over(w13).alias("last3_count_event_seq"),
        F.min("event_seq").over(w13).alias("last3_min_event_seq"),
        F.avg("event_seq").over(w13).alias("last3_avg_event_seq"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("last3_avg_event_seq") >= 7.6, "PREMIUM").otherwise("STANDARD").alias("last3_avg_event_seq_tier"),
        F.when(F.col("prev_last4_min_row_seq") >= 6.8, "HIGH").otherwise("LOW").alias("prev_last4_min_row_seq_tier"),
        F.when(F.col("next_last4_min_row_seq_percent_rank") >= 0.8, "LARGE").otherwise("SMALL").alias("next_last4_min_row_seq_percent_rank_tier"),
    )
    .agg(
        F.avg("event_seq").alias("avg_event_seq"),
        F.percentile_approx("first_known_last4_max_row_seq_2", [0.25, 0.5, 0.75]).alias("first_known_last4_max_row_seq_2_quartiles"),
        F.count("*").alias("cnt"),
    )
)
