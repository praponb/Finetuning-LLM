from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_transactions
    .groupBy(
        F.when(F.col("burst_id") >= 8, "LARGE").otherwise("SMALL").alias("burst_id_tier"),
    )
    .agg(
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.min("amount").alias("min_amount"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_rows"),
        F.countDistinct("txn_type").alias("distinct_txn_types"),
        F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_count"),
        F.count("*").alias("num_rows"),
    )
)
