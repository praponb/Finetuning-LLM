from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "customer_id").orderBy(F.col("opened_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("country").orderBy("balance", "id")
w3 = Window.partitionBy(F.date_trunc("week", "opened_date"))
w_hist = Window.orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

joined = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.city") == "Singapore")
    .select("s.*", F.col("d.city"), F.col("d.country"))
)

ranked = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    ranked
    .withColumn("prev_balance", F.lag("balance", 1).over(w2))
    .withColumn("opened_date_period_sum_balance", F.sum("balance").over(w3))
    .withColumn("first_known_city", F.first("city", ignorenulls=True).over(w_hist))
)

latest_match = (
    scored
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.balance",
        "s.rank_in_group",
        "s.prev_balance",
        "s.opened_date_period_sum_balance",
        "s.first_known_city",
        F.col("c.amount"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

joined_accounts = (
    latest_match
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id").eqNullSafe(F.col("d.id")), "inner")
    .select(
        "e.id",
        "e.customer_id",
        "e.account_type",
        "e.balance",
        "e.rank_in_group",
        "e.first_known_city",
        F.col("d.country"),
        F.col("d.email"),
    )
)

result = (
    joined_accounts
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "email",
        F.col("min_subscription_monthly_fee"),
    )
)
