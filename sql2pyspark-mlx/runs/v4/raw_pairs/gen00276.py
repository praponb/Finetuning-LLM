from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_payments
    .groupBy(
        "method",
        F.when(F.col("session_id") >= 8, "LARGE").otherwise("SMALL").alias("session_id_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("amount")).otherwise(0)).alias("refunded_amount"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.percentile_approx("session_id", [0.25, 0.5, 0.75]).alias("session_id_quartiles"),
    )
    .orderBy(F.col("span_minutes").desc())
)
