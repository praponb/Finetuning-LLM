from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_transactions
    .select(
        "id",
        "txn_type",
        "txn_time",
        "session_start_flag",
        F.when(F.col("txn_type").rlike("^(DEBIT|REFUND)$"), "EXTENDED").when(F.col("txn_type").rlike("^(CREDIT|FEE)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("txn_type_zone"),
    )
)
