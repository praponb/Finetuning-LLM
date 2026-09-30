from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "opened_date"), "status").orderBy(F.col("balance").desc())
w2 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_type").orderBy(F.col("balance").desc_nulls_first())
w5 = Window.partitionBy("customer_id").orderBy(F.col("first_known_balance").desc(), "id")
w6 = Window.partitionBy("account_type")
w7 = Window.orderBy("balance_dense_rank", "id").rowsBetween(-6, -1)
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w9 = Window.partitionBy("transaction_txn_type", "account_type").orderBy("balance_dense_rank_2", "id")
w10 = Window.partitionBy("transaction_txn_type").orderBy(F.col("first_known_balance_bucket").desc(), "id")
w11 = Window.partitionBy("transaction_txn_type").orderBy("balance_avg_6_prior", "id").rowsBetween(-3, Window.currentRow)

enriched = (
    df_accounts
    .filter(F.col("status") != "FROZEN")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        F.dense_rank().over(w).alias("balance_dense_rank"),
        F.sum("balance").over(w2).alias("cumulative_balance"),
        F.first("balance", ignorenulls=True).over(w3).alias("first_known_balance"),
        F.dense_rank().over(w4).alias("balance_dense_rank_2"),
    )
)

enriched_accounts = (
    enriched
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.balance",
        "s.balance_dense_rank",
        "s.cumulative_balance",
        "s.first_known_balance",
        "s.balance_dense_rank_2",
        F.col("b.city"),
        F.col("b.age"),
    )
)

enriched_accounts_2 = (
    enriched_accounts
    .select(
        "id",
        "account_type",
        "balance",
        "balance_dense_rank",
        "first_known_balance",
        "balance_dense_rank_2",
        "age",
        F.ntile(10).over(w5).alias("first_known_balance_bucket"),
        F.round((F.col("age") / F.sum("age").over(w6)), 4).alias("age_share"),
        F.avg("balance").over(w7).alias("balance_avg_6_prior"),
    )
)

latest_match = (
    enriched_accounts_2
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type").alias("transaction_txn_type"))
)

scored = (
    latest_match
    .select(
        "id",
        "account_type",
        "balance",
        "first_known_balance",
        "balance_dense_rank_2",
        "age",
        "first_known_balance_bucket",
        "balance_avg_6_prior",
        "transaction_txn_type",
        F.ntile(10).over(w9).alias("balance_dense_rank_2_bucket"),
        F.ntile(10).over(w10).alias("first_known_balance_bucket_bucket"),
        F.avg("balance_dense_rank").over(w11).alias("balance_dense_rank_avg_3"),
    )
)

result = (
    scored
    .groupBy("account_type")
    .agg(
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_rows"),
        F.round(F.sum("balance_dense_rank_avg_3") / F.nullif(F.count(F.when(F.col("transaction_txn_type") == "FEE", 1)), F.lit(0)), 2).alias("balance_dense_rank_avg_3_per_fee"),
        F.percentile_approx("balance_dense_rank_2", 0.95).alias("balance_dense_rank_2_p95"),
    )
    .orderBy("balance_dense_rank_avg_3_per_fee")
)
