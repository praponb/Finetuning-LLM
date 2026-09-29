from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("amount", "id")
w_part = Window.partitionBy("order_id")
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w5 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter(F.col("paid_at") > F.to_date(F.lit("2023-06-03")))
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        F.upper("status").alias("status_upper"),
        F.month("paid_at").alias("paid_at_month"),
        F.coalesce("method", F.lit("OTHER")).alias("method_filled"),
    )
)

scored = (
    base
    .withColumn("seq_no", F.row_number().over(w_grp))
    .withColumn("next_paid_at_month", F.lead("paid_at_month", 1, 0).over(w2))
    .withColumn("order_id_stddev_samp_paid_at_month", F.stddev_samp("paid_at_month").over(w_part))
)

flagged = (
    scored
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w4).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w4) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w5))

stats = (
    sessionized
    .groupBy(
        F.when(F.col("seq_no") >= 9, "LARGE").otherwise("SMALL").alias("seq_no_tier"),
        F.date_trunc("QUARTER", "paid_at").alias("quarter_start"),
    )
    .agg(
        F.percentile_approx("next_paid_at_month", [0.25, 0.5, 0.75]).alias("next_paid_at_month_quartiles"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.round(F.avg("next_paid_at_month"), 2).alias("avg_next_paid_at_month"),
        F.percentile_approx("order_id_stddev_samp_paid_at_month", 0.5).alias("order_id_stddev_samp_paid_at_month_p50"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
    )
)

result = (
    stats
    .select(
        "seq_no_tier",
        "quarter_start",
        "avg_seq_no",
        "avg_next_paid_at_month",
        "order_id_stddev_samp_paid_at_month_p50",
        "span_minutes",
    )
)
