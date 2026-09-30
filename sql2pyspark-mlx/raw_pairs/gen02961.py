from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.to_timestamp(F.lit("2023-09-28 13:00:00"))) & F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.round("amount", 0).alias("amount_rounded"),
    )
)

flagged = (
    base
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("amount") >= 4100, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        F.when(F.col("session_no") >= 7, "PREMIUM").otherwise("STANDARD").alias("session_no_tier"),
    )
    .agg(
        F.sum(F.when(F.col("merchant") == "Lazada", F.col("amount")).otherwise(0)).alias("lazada_amount"),
        F.count_if(F.col("txn_type") == "DEBIT").alias("debit_events"),
        F.max("txn_time").alias("ended_at"),
    )
    .filter(F.col("debit_events") >= 1)
)
