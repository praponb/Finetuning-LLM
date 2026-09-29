from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("is_new_burst") >= 1, "LARGE").otherwise("SMALL").alias("is_new_burst_tier"),
    )
    .agg(
        F.min("paid_at").alias("started_at"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "FAILED", 1)), F.lit(0)), 2).alias("amount_per_failed"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_rows"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("amount_per_failed") < 0)
)
