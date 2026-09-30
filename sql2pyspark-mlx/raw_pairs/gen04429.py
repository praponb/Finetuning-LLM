from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("amount") >= 3400, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.round(F.avg("is_new_burst"), 2).alias("avg_is_new_burst"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.min("txn_time").alias("started_at"),
    )
    .orderBy(F.col("amount_tier").desc_nulls_first())
)
