from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    sessions_transactions
    .groupBy("txn_type", F.date_trunc("YEAR", "txn_time").alias("year_start"))
    .agg(
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        F.countDistinct("merchant").alias("distinct_merchants"),
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("amount"))).alias("starbucks_total_amount"),
    )
    .filter(F.col("starbucks_total_amount") > 0)
)
