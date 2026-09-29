from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance", "id").rowsBetween(-6, -1)
w2 = Window.orderBy("balance")
w3 = Window.partitionBy("account_type").orderBy("balance", "id")
w4 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

cleaned = (
    df_accounts
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-07-26")))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        F.initcap(F.trim("account_type")).alias("account_type_title"),
        F.concat_ws(",", "account_type", "status").alias("account_type_status_key"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_clean"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_type").isin(["FEE", "CREDIT", "DEBIT"])), "left_semi")
)

scored = (
    eligible
    .select(
        "*",
        F.avg("balance").over(w).alias("balance_avg_6_prior"),
        F.dense_rank().over(w2).alias("balance_dense_rank"),
        F.lag("balance", 2).over(w3).alias("prev_balance"),
    )
)

scored_accounts = (
    scored
    .filter(F.col("account_type").isin(["CHECKING", "LOAN"]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "status_clean",
        "balance_avg_6_prior",
        "balance_dense_rank",
        "prev_balance",
        F.count("prev_balance").over(w4).alias("running_count_prev_balance"),
        F.max("prev_balance").over(w4).alias("running_max_prev_balance"),
        F.min("prev_balance").over(w4).alias("running_min_prev_balance"),
        F.avg("prev_balance").over(w4).alias("running_avg_prev_balance"),
    )
)

result = (
    scored_accounts
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance_dense_rank",
        "running_count_prev_balance",
        "running_max_prev_balance",
        "running_min_prev_balance",
        "running_avg_prev_balance",
        F.when(F.col("account_type").rlike("^(LOAN|CREDIT)$"), "CORE").when(F.col("account_type").rlike("^(SAVINGS|CHECKING)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("account_type_zone"),
        F.col("max_account_balance"),
    )
)
