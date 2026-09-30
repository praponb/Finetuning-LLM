from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status")
w2 = Window.partitionBy(F.year("opened_date"), "status")
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("amount").alias("max_order_amount"))

cleaned = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.when(F.col("status").isin(["CLOSED", "FROZEN"]), "Priority").otherwise("Other").alias("status_group"),
    )
)

with_window = (
    cleaned
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.opened_date") >= F.col("sub.start_date")) & (F.col("s.opened_date") <= F.date_add(F.col("sub.end_date"), 3)), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

not_flagged = (
    with_window
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.account_id").isin([2, 28, 30])), "left_anti")
)

matched_keys = (
    not_flagged
    .select("status", "id")
    .intersectAll(
        df_accounts
        .filter(F.col("account_type") != "LOAN")
        .select("status", "id")
    )
)

kept_rows = (
    not_flagged
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    kept_rows
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") >= F.col("s.opened_date")), "left_anti")
)

enriched = (
    eligible
    .select(
        "*",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4).alias("monthly_fee_share"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share_2"),
    )
)

result = (
    enriched
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "status_group",
        "subscription_id",
        "plan",
        "monthly_fee_share_2",
        F.when(F.col("status").rlike("^(OPEN|FROZEN)$"), "GROUP_A").otherwise("GROUP_B").alias("status_zone"),
        F.col("max_order_amount"),
    )
    .orderBy(F.col("max_order_amount").desc_nulls_last())
)
