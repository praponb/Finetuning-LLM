from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("txn_time") > F.lit("2023-09-28").cast("date"))
    .select(
        "*",
        F.concat_ws("-", "merchant", "txn_type").alias("merchant_txn_type_key"),
        F.concat("merchant", F.lit(" "), "txn_type").alias("merchant_txn_type"),
        F.coalesce("merchant", F.lit("OTHER")).alias("resolved_merchant"),
    )
)

flagged = (
    base
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

result = (
    sessionized
    .withColumn("session_no_tier", F.when(F.col("session_no") >= 6, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["session_no_tier", "session_no"], ["session_no_tier"], []],
        "session_no_tier",
        "session_no",
    )
    .agg(
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.count_if(F.col("txn_type") == "FEE").alias("fee_events"),
        F.max("session_no").alias("max_session_no"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("session_no")).otherwise(0)).alias("fee_session_no"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
    .orderBy(F.col("session_no_tier").desc_nulls_first())
)
