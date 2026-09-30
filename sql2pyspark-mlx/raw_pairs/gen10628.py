from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("account_type").orderBy("opened_date", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("customer_id").orderBy("transaction_txn_time_2", "id", "subscription_id")
w5 = Window.partitionBy("customer_id").orderBy("transaction_txn_time_2", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

attributed = (
    df_accounts
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & F.col("s.opened_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

scored = (
    attributed
    .filter(F.col("plan").isin(["basic", "standard", "enterprise"]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "subscription_id",
        F.min("balance").over(w_hist).alias("running_min_balance"),
        F.max("balance").over(w_hist).alias("running_max_balance"),
        F.count("balance").over(w_hist).alias("running_count_balance"),
    )
)

enriched = (
    scored
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.account_type",
        "e.opened_date",
        "e.subscription_id",
        "e.running_min_balance",
        "e.running_max_balance",
        "e.running_count_balance",
        F.col("b.age"),
        F.col("b.signup_date"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_type").alias("transaction_txn_type"),
        F.col("c.txn_time").alias("transaction_txn_time"),
    )
)

with_latest = (
    with_last
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.opened_date",
        "s.subscription_id",
        "s.running_max_balance",
        "s.signup_date",
        "s.transaction_txn_time",
        F.col("c.txn_time").alias("transaction_txn_time_2"),
    )
)

with_dims = (
    with_latest
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.opened_date",
        "src.subscription_id",
        "src.running_max_balance",
        "src.signup_date",
        "src.transaction_txn_time_2",
        F.col("c.name"),
    )
)

with_gaps = (
    with_dims
    .withColumn("new_group_flag", F.when(F.col("transaction_txn_time_2") - F.lag("transaction_txn_time_2").over(w4) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_accounts = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w5))

result = (
    sessions_accounts
    .select(
        "id",
        "customer_id",
        "subscription_id",
        "running_max_balance",
        "transaction_txn_time_2",
        "name",
        "new_group_flag",
        "session_id",
    )
    .orderBy(F.col("session_id").desc())
)
