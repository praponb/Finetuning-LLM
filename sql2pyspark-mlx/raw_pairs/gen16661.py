from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("method").orderBy("amount_filled", "id")
w3 = Window.partitionBy(F.month("paid_at"), "method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("method").orderBy("paid_at", "id")
w5 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("method").orderBy(F.col("is_new_burst").asc_nulls_first())
w7 = Window.partitionBy(F.date_trunc("YEAR", "paid_at"), "method")
w8 = Window.partitionBy("method").orderBy("next_amount_filled")
w9 = Window.partitionBy("method").orderBy("is_new_burst", "id").rowsBetween(-6, -1)
w10 = Window.partitionBy("method").orderBy("is_new_burst", "id").rowsBetween(-3, Window.currentRow)
w11 = Window.partitionBy("method").orderBy("is_new_burst_percent_rank", "id").rowsBetween(-2, Window.currentRow)
w12 = Window.partitionBy("method").orderBy("is_new_burst", "id")
w13 = Window.partitionBy("method").orderBy("is_new_burst_percent_rank", "id")

filtered = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-10-09 17:00:00").cast("timestamp"))
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        F.date_format("paid_at", "yyyy-MM-dd").alias("paid_at_label"),
    )
)

cleaned = (
    filtered
    .filter((F.col("paid_at") >= F.lit("2023-09-29").cast("date")) & F.col("method").isNotNull())
    .select(
        "id",
        "method",
        "paid_at",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "method",
        "paid_at",
        F.first("method", ignorenulls=True).over(w).alias("first_known_method"),
        F.lead("amount_filled", 1).over(w_recent).alias("next_amount_filled"),
        F.last("method", ignorenulls=True).over(w3).alias("last_known_method"),
    )
)

flagged = (
    metrics
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w4).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w4) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w5))

with_windows = (
    sessionized
    .filter(~F.col("method").isin(["bank_transfer", "cash"]))
    .select(
        "*",
        F.percent_rank().over(w6).alias("is_new_burst_percent_rank"),
        F.sum("is_new_burst").over(w7).alias("paid_at_period_sum_is_new_burst"),
        F.rank().over(w8).alias("next_amount_filled_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "method",
        "paid_at",
        "first_known_method",
        "last_known_method",
        "is_new_burst",
        "burst_id",
        "is_new_burst_percent_rank",
        "next_amount_filled_rank",
        F.avg("next_amount_filled").over(w9).alias("next_amount_filled_avg_6_prior"),
        F.avg("is_new_burst_percent_rank").over(w10).alias("is_new_burst_percent_rank_avg_3"),
        F.avg("is_new_burst_percent_rank").over(w11).alias("is_new_burst_percent_rank_avg_2"),
        F.ntile(5).over(w12).alias("is_new_burst_bucket"),
        F.lead("next_amount_filled_rank", 1).over(w13).alias("next_next_amount_filled_rank"),
    )
)
