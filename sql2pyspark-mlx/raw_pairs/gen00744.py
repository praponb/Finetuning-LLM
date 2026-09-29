from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("method").eqNullSafe(F.lit("bank_transfer")))
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.dayofmonth("paid_at").alias("paid_at_day"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_filled"),
    )
)

flagged = (
    valid
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_payments = (
    flagged
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        "paid_at_day",
        "method_filled",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessions_payments
    .groupBy(
        F.when(F.col("amount") >= 1230, "HIGH").otherwise("LOW").alias("amount_tier"),
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
    )
    .agg(
        F.max("paid_at").alias("last_paid_at"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "SUCCESS", 1)), F.lit(0)), 2).alias("amount_per_success"),
        F.max("paid_at").alias("last_seen"),
    )
)
