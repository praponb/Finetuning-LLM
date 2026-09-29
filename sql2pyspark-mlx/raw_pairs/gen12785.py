from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("opened_date", "id")

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("txn_time") > F.lit("2023-11-11").cast("date")))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.round((F.col("amount") * 1.15), 2).alias("adjusted_amount"),
    )
)

with_dims = (
    valid
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.account_id",
        "a.txn_time",
        "a.is_flagged",
        F.col("d.opened_date"),
        F.col("d.status"),
    )
)

with_gaps = (
    with_dims
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

scored = (
    sessions_transactions
    .withColumn("prev_session_id", F.lag("session_id").over(w3))
    .withColumn("next_session_id", F.lead("session_id").over(w3))
    .withColumn("prev_new_group_flag", F.lag("new_group_flag").over(w3))
    .withColumn("next_new_group_flag", F.lead("new_group_flag").over(w3))
    .withColumn("event_seq", F.row_number().over(w3))
)

result = (
    scored
    .groupBy("account_id")
    .pivot("status", ["OPEN", "CLOSED"])
    .agg(F.avg("new_group_flag"))
)
