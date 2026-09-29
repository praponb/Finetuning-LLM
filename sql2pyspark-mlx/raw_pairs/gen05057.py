from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("method").orderBy("paid_at_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("method").orderBy(F.col("running_max_amount").asc_nulls_first())
w6 = Window.partitionBy("method").orderBy("paid_at", "id")

filtered = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        F.col("paid_at").cast("date").alias("paid_at_day"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_filled"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.avg("amount").over(w_running).alias("running_avg_amount"),
        F.max("amount").over(w_running).alias("running_max_amount"),
        F.min("amount").over(w_running).alias("running_min_amount"),
    )
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

enriched_payments = (
    ranked
    .select(
        "id",
        "method",
        "paid_at",
        "running_max_amount",
        F.avg("running_avg_amount").over(w3).alias("running_avg_amount_avg_2_prior"),
        F.first("running_max_amount", ignorenulls=True).over(w4).alias("first_known_running_max_amount"),
        F.cume_dist().over(w5).alias("running_max_amount_cume_dist"),
    )
)

flagged = (
    enriched_payments
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w6).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w6) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w_running))

result = (
    islands
    .groupBy(F.date_trunc("YEAR", "paid_at").alias("year_start"))
    .agg(
        F.size(F.collect_set("method")).alias("num_methods"),
        F.count_if(F.col("method") == "bank_transfer").alias("bank_transfer_events"),
        F.round(F.avg("running_max_amount"), 2).alias("avg_running_max_amount"),
        F.percentile_approx("running_max_amount", 0.95).alias("running_max_amount_p95"),
    )
    .orderBy("bank_transfer_events")
)
