from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("amount") >= 1370, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        F.date_trunc("QUARTER", "paid_at").alias("quarter_start"),
    )
    .agg(
        F.max("island_id").alias("max_island_id"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_rows"),
        F.min("paid_at").alias("first_seen"),
        F.percentile_approx("island_id", [0.25, 0.5, 0.75]).alias("island_id_quartiles"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("is_new_burst")).otherwise(0)).alias("success_is_new_burst"),
    )
)
