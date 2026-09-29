from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy(F.col("balance").asc_nulls_first())
w2 = Window.partitionBy("account_type", "customer_id").orderBy("balance")
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("account_type")
w_ordered = Window.partitionBy("customer_id").orderBy(F.col("balance").asc_nulls_first())

kept_keys = (
    df_accounts
    .select("customer_id")
    .intersectAll(
        df_accounts
        .filter(F.col("opened_date").between("2024-11-13", "2025-01-06"))
        .select("customer_id")
    )
)

kept_rows = (
    df_accounts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_anti")
)

metrics = (
    not_flagged
    .withColumn("balance_dense_rank", F.dense_rank().over(w))
    .withColumn("balance_rank", F.rank().over(w2))
    .withColumn("customer_id_max_balance", F.max("balance").over(w3))
    .withColumn("account_type_stddev_samp_balance", F.stddev_samp("balance").over(w4))
    .withColumn("balance_percent_rank", F.percent_rank().over(w_ordered))
)

result = (
    metrics
    .groupBy(F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.round(F.sum("balance_rank") / F.nullif(F.count(F.when(F.col("account_type") == "CHECKING", 1)), F.lit(0)), 2).alias("balance_rank_per_checking"),
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_count"),
        F.round(F.avg("balance_percent_rank"), 2).alias("avg_balance_percent_rank"),
        F.avg(F.when(F.col("account_type") == "SAVINGS", F.col("balance_dense_rank"))).alias("savings_avg_balance_dense_rank"),
        F.round(F.avg("customer_id_max_balance"), 2).alias("avg_customer_id_max_balance"),
    )
)
