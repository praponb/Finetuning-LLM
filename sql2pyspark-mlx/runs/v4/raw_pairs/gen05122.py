from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(F.date_trunc("week", "txn_time").alias("week_start"), "island_id")
    .agg(
        F.avg("is_new_burst").alias("avg_is_new_burst"),
        F.count(F.when(F.col("merchant") == "Lazada", 1)).alias("lazada_count"),
        F.max("island_id").alias("max_island_id"),
    )
    .orderBy("avg_is_new_burst")
)
