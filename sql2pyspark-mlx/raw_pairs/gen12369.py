from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("merchant").isin(["Lazada", "Shell", "7-Eleven"]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "is_flagged",
        F.round((F.col("amount") * 1.07), 2).alias("adjusted_amount"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_filled"),
    )
)

flagged = (
    cleaned
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessions_transactions
    .cube(
        F.when(F.col("adjusted_amount") >= 85, "LARGE").otherwise("SMALL").alias("adjusted_amount_tier"),
        F.when(F.col("is_new_session") >= 1, "LARGE").otherwise("SMALL").alias("is_new_session_tier"),
    )
    .agg(
        F.avg(F.when(F.col("txn_type") == "CREDIT", F.col("session_id"))).alias("credit_avg_session_id"),
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("adjusted_amount", [0.25, 0.5, 0.75]).alias("adjusted_amount_quartiles"),
    )
    .orderBy(F.col("credit_avg_session_id").desc_nulls_first())
)
