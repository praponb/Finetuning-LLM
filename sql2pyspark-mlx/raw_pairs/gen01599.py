from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "is_flagged",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

result = (
    sessionized
    .rollup(
        "account_id",
        "session_no",
        F.when(F.col("session_no") >= 8, "HIGH").otherwise("LOW").alias("session_no_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("merchant") == "Lazada", 1)).alias("lazada_rows"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.size(F.collect_set("merchant")).alias("num_merchants_2"),
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("merchant") == "Starbucks", 1)), F.lit(0)), 2).alias("session_no_per_starbucks"),
        F.min("session_no").alias("min_session_no"),
    )
)
