from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance")
w2 = Window.partitionBy("account_type", "customer_id")
w3 = Window.orderBy(F.col("balance_percent_rank").desc_nulls_last())
w4 = Window.orderBy(F.col("balance_percent_rank").asc_nulls_last())
w5 = Window.orderBy("balance_percent_rank_dense_rank")
w6 = Window.orderBy(F.col("balance_percent_rank_dense_rank").desc())
w7 = Window.orderBy(F.col("balance_percent_rank").desc())
w8 = Window.orderBy("balance_percent_rank")

remaining_keys = (
    df_accounts
    .select("account_type", "status")
    .subtract(
        df_accounts
        .filter(F.col("customer_id").isin([2, 16, 26, 40]))
        .select("account_type", "status")
    )
)

verified = (
    df_accounts
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "id",
        "opened_date",
        F.percent_rank().over(w).alias("balance_percent_rank"),
        F.max("balance").over(w2).alias("account_type_max_balance"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.amount") < 1500), "left_semi")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") >= F.col("s.opened_date")), "left_anti")
)

enriched = (
    not_flagged
    .filter(F.col("account_type_max_balance") == 71715)
    .select(
        "*",
        F.dense_rank().over(w3).alias("balance_percent_rank_dense_rank"),
        F.rank().over(w4).alias("balance_percent_rank_rank"),
    )
)

not_flagged_accounts = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.hour(F.col("c.txn_time")).between(10, 15)), "left_anti")
)

enriched_accounts = (
    not_flagged_accounts
    .select(
        "*",
        F.dense_rank().over(w5).alias("balance_percent_rank_dense_rank_dense_rank"),
        F.dense_rank().over(w6).alias("balance_percent_rank_dense_rank_dense_rank_2"),
        F.rank().over(w7).alias("balance_percent_rank_rank_2"),
        F.percent_rank().over(w8).alias("balance_percent_rank_percent_rank"),
    )
)

result = (
    enriched_accounts
    .select(
        "id",
        "opened_date",
        "balance_percent_rank",
        "account_type_max_balance",
        "balance_percent_rank_dense_rank",
        "balance_percent_rank_rank",
        "balance_percent_rank_dense_rank_dense_rank",
        "balance_percent_rank_dense_rank_dense_rank_2",
        "balance_percent_rank_percent_rank",
    )
)
