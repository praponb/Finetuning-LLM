from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_transactions
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("is_new_burst") >= 1, "HIGH").otherwise("LOW").alias("is_new_burst_tier"),
    )
    .agg(
        F.round(F.sum("is_new_burst") / F.nullif(F.count(F.when(F.col("merchant") == "Shell", 1)), F.lit(0)), 2).alias("is_new_burst_per_shell"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("session_id")).otherwise(0)).alias("credit_session_id"),
        F.percentile_approx("is_new_burst", [0.25, 0.5, 0.75]).alias("is_new_burst_quartiles"),
        F.min("txn_time").alias("first_seen"),
    )
)
