from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("burst_id") >= 7, "PREMIUM").otherwise("STANDARD").alias("burst_id_tier"),
        F.when(F.col("is_new_burst") >= 1, "LARGE").otherwise("SMALL").alias("is_new_burst_tier"),
    )
    .agg(
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("is_new_burst"))).alias("bank_transfer_total_is_new_burst"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("is_new_burst", [0.25, 0.5, 0.75]).alias("is_new_burst_quartiles"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
    .orderBy(F.col("is_new_burst_tier").desc())
)
