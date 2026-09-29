from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("merchant") == "Shell"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.col("txn_time").cast("date").alias("txn_time_day"),
    )
)

gaps = (
    valid
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessions_transactions
    .groupBy("txn_type")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.max("txn_time").alias("ended_at"),
    )
    .orderBy("txn_type")
)
