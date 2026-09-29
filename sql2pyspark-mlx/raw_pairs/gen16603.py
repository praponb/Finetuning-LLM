from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessionized
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "island_id",
        F.when(F.col("merchant").rlike("^(Lazada|Starbucks)$"), "CORE").when(F.col("merchant").rlike("^(Grab|7-Eleven)$"), "GROUP_A").otherwise("EXTENDED").alias("merchant_zone"),
    )
    .orderBy(F.col("island_id").desc_nulls_last())
)
