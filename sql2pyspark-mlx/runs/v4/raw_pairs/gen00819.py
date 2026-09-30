from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessions_transactions
    .withColumn("quarter_start", F.date_trunc("QUARTER", "txn_time"))
    .withColumn("amount_tier", F.when(F.col("amount") >= 3600, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["quarter_start", "amount_tier"], ["quarter_start"], []],
        "quarter_start",
        "amount_tier",
    )
    .agg(
        F.percentile_approx("is_new_session", [0.25, 0.5, 0.75]).alias("is_new_session_quartiles"),
        F.count_if(F.col("merchant") == "Shell").alias("shell_events"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.grouping("quarter_start").alias("grouping_id"),
    )
    .orderBy(F.col("num_merchants").desc_nulls_last())
)
