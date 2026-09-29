from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id").orderBy(F.col("balance").desc_nulls_first())
w3 = Window.partitionBy("customer_id", "account_type")

kept = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.account_id").isin([17, 37])), "left_anti")
)

latest_match = (
    kept
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("transaction_amount"))
)

matched_keys = (
    latest_match
    .select("account_type")
    .exceptAll(
        df_accounts
        .filter(F.col("customer_id").isin([2, 18]))
        .select("account_type")
    )
)

kept_rows = (
    latest_match
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .withColumn("balance_percent_rank", F.percent_rank().over(w2))
    .withColumn("customer_id_sum_balance", F.sum("balance").over(w3))
)

result = (
    scored
    .rollup("status")
    .agg(
        F.max(F.when(F.col("status") == "CLOSED", F.col("balance_percent_rank"))).alias("closed_max_balance_percent_rank"),
        F.round(F.avg("customer_id_sum_balance"), 2).alias("avg_customer_id_sum_balance"),
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"),
    )
    .filter(F.col("avg_customer_id_sum_balance") > 0)
    .orderBy("status")
)
