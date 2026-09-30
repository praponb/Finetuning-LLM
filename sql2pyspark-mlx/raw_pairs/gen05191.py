from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy(F.month("opened_date_period"), "customer_id")
w_part = Window.orderBy(F.col("opened_date_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_hist_2 = Window.partitionBy("status").orderBy(F.col("opened_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

valid = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "SAVINGS", "CHECKING"]))
    .select(
        "*",
        F.date_trunc("YEAR", "opened_date").alias("opened_date_period"),
        F.substring("status", 1, 4).alias("status_prefix"),
    )
)

matched_keys = (
    valid
    .select("customer_id", "status")
    .intersect(
        df_accounts
        .filter(F.col("customer_id").isin([1, 27]))
        .select("customer_id", "status")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.opened_date_period")), "left_anti")
)

scored = (
    eligible
    .select(
        "account_type",
        "balance",
        "opened_date",
        "opened_date_period",
        "status_prefix",
        F.min("balance").over(w_hist).alias("opened_date_period_month_min_balance"),
        F.avg("balance").over(w_part).alias("balance_avg_90d"),
        F.count("balance").over(w_hist_2).alias("balance_count_30d"),
    )
)

valid_accounts = (
    scored
    .filter(F.col("opened_date") > F.lit("2023-03-06").cast("date"))
    .select(
        "account_type",
        "balance",
        "status_prefix",
        "opened_date_period_month_min_balance",
        F.when(F.col("opened_date_period_month_min_balance") >= 67265, "High").when(F.col("opened_date_period_month_min_balance") >= 10051, "Medium").otherwise("Low").alias("opened_date_period_month_min_balance_band"),
        F.upper("account_type").alias("account_type_upper"),
    )
)

valid_accounts_2 = (
    valid_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "LOAN", "CHECKING"]))
    .withColumn("adjusted_opened_date_period_month_min_balance", F.round((F.col("opened_date_period_month_min_balance") * 1.07), 2))
    .withColumn("clean_account_type", F.trim("account_type"))
)

result = (
    valid_accounts_2
    .select(
        "account_type",
        "balance",
        "status_prefix",
        "opened_date_period_month_min_balance",
        "opened_date_period_month_min_balance_band",
        "account_type_upper",
        F.when(F.col("account_type").rlike("^(LOAN|SAVINGS)$"), "PRIORITY").otherwise("GROUP_B").alias("account_type_zone"),
    )
    .orderBy("account_type_zone")
)
