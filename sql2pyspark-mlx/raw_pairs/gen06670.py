from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("is_loan").desc())
w2 = Window.partitionBy(F.month("opened_date"), "account_type").orderBy(F.col("balance").desc())
w3 = Window.partitionBy("customer_id", "status")
w4 = Window.partitionBy("account_type")
w5 = Window.partitionBy(F.month("opened_date"))
w6 = Window.partitionBy("status")
w7 = Window.partitionBy(F.year("opened_date"), "status").orderBy(F.col("balance").desc())

cleaned = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "FROZEN", "OPEN"]))
    .select(
        "*",
        F.when(F.col("account_type") == "LOAN", 1).otherwise(0).alias("is_loan"),
    )
)

kept_keys = (
    cleaned
    .select("account_type", "id")
    .intersectAll(
        df_accounts
        .filter(F.col("status") == "OPEN")
        .select("account_type", "id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "id",
        "balance",
        "opened_date",
        "status",
        F.dense_rank().over(w).alias("is_loan_dense_rank"),
        F.rank().over(w2).alias("balance_rank"),
        F.stddev_samp("balance").over(w3).alias("customer_id_stddev_samp_balance"),
        F.avg("is_loan").over(w4).alias("account_type_avg_is_loan"),
    )
)

scored_accounts = (
    scored
    .select(
        "id",
        "opened_date",
        "is_loan_dense_rank",
        "balance_rank",
        "customer_id_stddev_samp_balance",
        F.max("is_loan_dense_rank").over(w5).alias("opened_date_month_max_is_loan_dense_rank"),
        F.stddev_samp("balance_rank").over(w6).alias("status_stddev_samp_balance_rank"),
        F.cume_dist().over(w7).alias("balance_cume_dist"),
    )
)

kept = (
    scored_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.merchant").isin(["Amazon", "Shell"])), "left_anti")
)

remaining_keys = (
    kept
    .select("id")
    .intersect(
        df_accounts
        .filter(F.col("status") != "FROZEN")
        .select("id")
    )
)

kept_rows_accounts = (
    kept
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows_accounts
    .groupBy(
        F.when(F.col("balance_cume_dist") >= 0.72, "PREMIUM").otherwise("STANDARD").alias("balance_cume_dist_tier"),
    )
    .agg(
        F.round(F.avg("is_loan_dense_rank"), 2).alias("avg_is_loan_dense_rank"),
        F.round(F.avg("balance_rank"), 2).alias("avg_balance_rank"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("avg_is_loan_dense_rank") < 1000000)
)
