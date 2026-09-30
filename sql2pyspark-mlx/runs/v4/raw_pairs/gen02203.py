from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .withColumn("quarter_start", F.date_trunc("QUARTER", "prev_paid_at"))
    .withColumn("is_new_burst_tier", F.when(F.col("is_new_burst") >= 1, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["quarter_start", "is_new_burst_tier"], ["quarter_start"], []],
        "quarter_start",
        "is_new_burst_tier",
    )
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.avg(F.when(F.col("status") == "SUCCESS", F.col("amount"))).alias("success_avg_amount"),
        F.percentile_approx("session_id", 0.5).alias("session_id_p50"),
    )
    .filter(F.col("amount_stddev") >= 0)
    .orderBy(F.col("session_id_p50").desc())
)
