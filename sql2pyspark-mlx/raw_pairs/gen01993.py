from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("amount") >= 4500, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.percentile_approx("session_id", 0.9).alias("session_id_p90"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_rows"),
        F.round(F.avg("session_id"), 2).alias("avg_session_id"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
    .filter(F.col("row_count") > 1)
    .orderBy(F.col("session_id_p90").asc_nulls_last())
)
