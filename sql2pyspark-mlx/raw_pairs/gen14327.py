from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_transactions = (
    gaps
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "is_flagged",
        "burst_id",
        F.when(F.col("txn_type").rlike("^(REFUND|FEE)$"), "CORE").otherwise("GROUP_B").alias("txn_type_zone"),
    )
    .orderBy(F.col("txn_type").desc_nulls_first())
)
