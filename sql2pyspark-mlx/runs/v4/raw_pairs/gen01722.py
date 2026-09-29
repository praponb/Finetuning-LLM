from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_grp_2 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_grp_3 = Window.partitionBy("status")
w4 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

with_dims = (
    df_transactions
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "inner")
    .select("src.*", F.col("a.status"))
)

remaining_keys = (
    with_dims
    .select("id", "txn_type")
    .subtract(
        df_transactions
        .filter(F.col("txn_type") == "REFUND")
        .select("id", "txn_type")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("amount_count_7d", F.count("amount").over(w_grp))
    .withColumn("amount_max_7d", F.max("amount").over(w_grp_2))
    .withColumn("status_stddev_samp_amount", F.stddev_samp("amount").over(w_grp_3))
    .withColumn("amount_avg_90d", F.avg("amount").over(w4))
)

result = (
    enriched
    .groupBy("account_id")
    .agg(
        F.countDistinct("account_id").alias("distinct_account_ids"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("status_stddev_samp_amount")).otherwise(0)).alias("frozen_status_stddev_samp_amount"),
        F.countDistinct("account_id").alias("distinct_account_ids_2"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.max("txn_time").alias("ended_at"),
    )
    .filter(F.col("num_status") >= 1)
    .orderBy(F.col("num_status").desc_nulls_last())
)
