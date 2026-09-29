from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "prev_txn_time",
        "is_new_burst",
        F.when(F.col("txn_type").rlike("^(REFUND|DEBIT)$"), "GROUP_B").otherwise("EXTENDED").alias("txn_type_zone"),
    )
    .orderBy("merchant")
)
