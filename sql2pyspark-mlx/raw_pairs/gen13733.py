from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.orderBy(F.col("balance").desc())
w3 = Window.partitionBy("customer_id", "account_type").orderBy(F.col("balance").asc_nulls_last())

cleaned = (
    df_accounts
    .filter((F.col("status").eqNullSafe(F.lit("OPEN"))) & (F.col("opened_date") >= F.lit("2023-05-27").cast("date")))
    .select(
        "*",
        F.upper("account_type").alias("account_type_upper"),
        F.round("balance", 2).alias("balance_rounded"),
    )
)

remaining_keys = (
    cleaned
    .select("account_type", "id")
    .subtract(
        df_accounts
        .filter(F.col("account_type").isin(["CHECKING", "LOAN", "CREDIT"]))
        .select("account_type", "id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") >= F.col("s.opened_date")), "left_semi")
)

filtered = (
    kept
    .filter(F.col("status").isin(["CLOSED", "FROZEN", "OPEN"]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        "account_type_upper",
        "balance_rounded",
        F.substring("account_type", 1, 2).alias("account_type_prefix"),
        F.lower("status").alias("status_lower"),
    )
)

with_windows = (
    filtered
    .filter(F.col("account_type") == "SAVINGS")
    .select(
        "customer_id",
        "account_type",
        "status",
        "balance_rounded",
        "account_type_prefix",
        "status_lower",
        F.max("balance_rounded").over(w).alias("status_max_balance_rounded"),
        F.rank().over(w2).alias("balance_rank"),
        F.percent_rank().over(w3).alias("balance_percent_rank"),
    )
)

result = (
    with_windows
    .rollup("status")
    .agg(
        F.count_if(F.col("status") == "CLOSED").alias("closed_events"),
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_rows"),
        F.round(F.avg("balance_rounded"), 2).alias("avg_balance_rounded"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("closed_events") >= 1)
    .orderBy("loan_rows")
)
