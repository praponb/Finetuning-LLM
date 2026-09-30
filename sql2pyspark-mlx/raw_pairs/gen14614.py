from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy(F.date_trunc("YEAR", "txn_time").alias("year_start"))
    .agg(
        F.percentile_approx("session_start_flag", 0.5).alias("session_start_flag_p50"),
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_rows"),
        F.countDistinct("txn_type").alias("distinct_txn_types"),
        F.count_if(F.col("txn_type") == "FEE").alias("fee_events"),
        F.max("txn_time").alias("ended_at"),
    )
    .filter(F.col("distinct_txn_types") > 1)
    .orderBy(F.col("distinct_txn_types").desc_nulls_first())
)
