from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id")
w2 = Window.partitionBy(F.date_trunc("month", "ends_on")).orderBy("ends_on", "id")
w3 = Window.orderBy(F.col("starts_on").desc(), "id")
w_part = Window.orderBy("days_ends_on_to_starts_on", "id").rowsBetween(-3, Window.currentRow)
w_running = Window.orderBy("seq_no", "id").rowsBetween(-2, Window.currentRow)

deduped = (
    df_promotions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "starts_on", "ends_on")
)

scored = (
    deduped
    .select(
        "id",
        "starts_on",
        "ends_on",
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

base = (
    scored
    .filter(F.col("starts_on") >= F.to_date(F.lit("2023-06-05")))
    .select(
        "id",
        "starts_on",
        "ends_on",
        "event_seq",
        "seq_no",
        F.datediff("starts_on", "ends_on").alias("days_ends_on_to_starts_on"),
        F.greatest("seq_no", "event_seq").alias("greatest_seq_no_event_seq"),
    )
)

metrics = (
    base
    .filter(F.month("ends_on").isin([2, 5, 11]))
    .withColumn("last4_sum_event_seq", F.sum("event_seq").over(w_part))
    .withColumn("last4_avg_event_seq", F.avg("event_seq").over(w_part))
    .withColumn("last4_min_event_seq", F.min("event_seq").over(w_part))
    .withColumn("last4_count_event_seq", F.count("event_seq").over(w_part))
    .withColumn("event_seq_avg_2", F.avg("event_seq").over(w_running))
)

result = (
    metrics
    .select(
        "id",
        "last4_min_event_seq",
        "last4_avg_event_seq",
        "last4_sum_event_seq",
        "greatest_seq_no_event_seq",
    )
    .unpivot(
        ids=["id"],
        values=["last4_min_event_seq", "last4_avg_event_seq", "last4_sum_event_seq", "greatest_seq_no_event_seq"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
