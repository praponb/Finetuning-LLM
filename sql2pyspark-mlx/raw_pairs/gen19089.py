from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

result = (
    islands
    .groupBy(
        F.when(F.col("amount") >= 3000, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.min("txn_time").alias("first_seen"),
        F.avg(F.when(F.col("txn_type") == "FEE", F.col("amount"))).alias("fee_avg_amount"),
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.count_if(F.col("txn_type") == "REFUND").alias("refund_events"),
        F.avg("amount").alias("avg_amount"),
    )
    .orderBy(F.col("refund_events").desc())
)
