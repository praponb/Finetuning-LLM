from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_transactions
    .groupBy(F.date_trunc("year", "txn_time").alias("year_start"))
    .agg(
        F.countDistinct("merchant").alias("distinct_merchants"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("island_id")).otherwise(0)).alias("fee_island_id"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("merchant") == "Lazada", 1)).alias("lazada_rows"),
    )
    .orderBy(F.col("avg_amount").desc_nulls_first())
)
