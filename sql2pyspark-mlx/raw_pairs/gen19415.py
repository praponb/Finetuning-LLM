from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(~F.col("txn_type").isin(["CREDIT", "REFUND", "DEBIT"]))
    .select(
        "*",
        F.when(F.col("merchant").isin(["Starbucks", "Lazada"]), "Priority").otherwise("Other").alias("merchant_group"),
    )
)

with_prev = filtered.withColumn("prev_txn_time", F.lag("txn_time").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("session_no") >= 9, "LARGE").otherwise("SMALL").alias("session_no_tier"),
        "merchant",
        F.date_trunc("QUARTER", "prev_txn_time").alias("quarter_start"),
    )
    .agg(
        ((F.unix_timestamp(F.max("prev_txn_time")) - F.unix_timestamp(F.min("prev_txn_time"))) / 60).alias("duration_min"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.min("txn_time").alias("first_seen"),
        F.round(F.sum("is_new_session") / F.nullif(F.count(F.when(F.col("txn_type") == "CREDIT", 1)), F.lit(0)), 2).alias("is_new_session_per_credit"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("duration_min") >= 0)
)
