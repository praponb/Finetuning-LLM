from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("amount") >= 1740, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.max(F.when(F.col("method") == "card", F.col("is_new_burst"))).alias("card_max_is_new_burst"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("status") == "FAILED", 1)), F.lit(0)), 2).alias("session_id_per_failed"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("session_id")).otherwise(0)).alias("refunded_session_id"),
    )
    .filter(F.col("refunded_session_id") > 0)
    .orderBy(F.col("failed_count").desc_nulls_first())
)
