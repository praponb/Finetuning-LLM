from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy("merchant")
    .agg(
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
        F.min("txn_time").alias("first_txn_time"),
        F.sum(F.when(F.col("merchant") == "Lazada", F.col("island_id")).otherwise(0)).alias("lazada_island_id"),
    )
    .orderBy(F.col("credit_count").desc_nulls_last())
)
