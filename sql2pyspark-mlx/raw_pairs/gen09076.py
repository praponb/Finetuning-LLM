from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

flagged = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy("merchant")
    .agg(
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.min("txn_time").alias("first_txn_time"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
    )
    .filter(F.col("debit_count") < 1000)
    .orderBy(F.col("num_merchants").desc())
)
