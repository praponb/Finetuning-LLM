from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("month", "opened_date"), "account_type")
w_ordered = Window.partitionBy(F.month("opened_date"))
w3 = Window.partitionBy(F.year("opened_date"), "customer_id")
w4 = Window.partitionBy("customer_id")

cleaned = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (~F.col("status").eqNullSafe(F.lit("FROZEN"))))
    .select(
        "*",
        F.when(F.col("status").isin(["OPEN", "FROZEN"]), "Tier 1").otherwise("Other").alias("status_group"),
        F.when(F.col("status").isin(["CLOSED", "OPEN"]), "Priority").otherwise("Other").alias("status_group_2"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_clean"),
    )
)

kept_keys = (
    cleaned
    .select("account_type", "status")
    .intersectAll(
        df_accounts
        .filter(F.col("account_type") == "LOAN")
        .select("account_type", "status")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .filter(F.col("balance") <= 77000)
    .withColumn("opened_date_period_avg_balance", F.avg("balance").over(w))
    .withColumn("opened_date_month_min_balance", F.min("balance").over(w_ordered))
)

kept = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left_anti")
)

joined = (
    kept
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select(
        "a.customer_id",
        "a.balance",
        "a.opened_date",
        "a.status_group",
        "a.status_clean",
        "a.opened_date_period_avg_balance",
        F.col("d.credit_limit"),
        F.col("d.signup_date"),
    )
)

enriched = (
    joined
    .select(
        "customer_id",
        "opened_date",
        "credit_limit",
        "signup_date",
        F.max("balance").over(w3).alias("opened_date_year_max_balance"),
        F.avg("opened_date_period_avg_balance").over(w4).alias("customer_id_avg_opened_date_period_avg_balance"),
        F.stddev_samp("credit_limit").over(w4).alias("customer_id_stddev_samp_credit_limit"),
    )
)

result = (
    enriched
    .select(
        "opened_date",
        "credit_limit",
        "signup_date",
        "opened_date_year_max_balance",
        "customer_id_avg_opened_date_period_avg_balance",
        "customer_id_stddev_samp_credit_limit",
    )
)
