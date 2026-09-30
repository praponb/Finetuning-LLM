from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("txn_time") > F.to_timestamp(F.lit("2023-09-28 13:00:00")))
    .withColumn("txn_time_date", F.to_date("txn_time"))
)

with_prev = valid.withColumn("prev_txn_time", F.lag("txn_time").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_transactions
    .rollup(
        F.date_trunc("YEAR", "prev_txn_time").alias("year_start"),
        F.when(F.col("amount") >= 3200, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.percentile_approx("is_new_burst", 0.9).alias("is_new_burst_p90"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_rows"),
    )
    .orderBy(F.col("year_start").desc())
)
