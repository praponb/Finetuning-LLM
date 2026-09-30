from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy("opened_date", "id")
w2 = Window.partitionBy("status").orderBy("txn_time", "id")
w3 = Window.partitionBy("status").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.opened_date"), F.col("b.balance"), F.col("b.status"))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "left")
    .select("e.*", F.col("a.opened_date").alias("account_opened_date"))
)

deduped = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

gaps = (
    deduped
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w2).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w2) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("island_id", F.sum("new_group_flag").over(w3))

summary = (
    sessions_transactions
    .groupBy(
        F.when(F.col("balance") >= 80000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
    )
    .agg(
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_rows"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("island_id"))).alias("frozen_total_island_id"),
        F.min("txn_time").alias("first_seen"),
    )
)

result = (
    summary
    .select("balance_tier", "debit_rows", "duration_min", "first_seen")
    .orderBy("debit_rows", "duration_min")
)
