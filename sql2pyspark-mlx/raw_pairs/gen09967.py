from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("transaction_merchant").orderBy(F.col("balance").desc(), "id", "subscription_id")

attributed = (
    df_accounts
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & F.col("src.opened_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_latest = (
    attributed
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount"), F.col("c.merchant").alias("transaction_merchant"))
)

enriched = (
    with_latest
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .filter(F.col("b.is_active"))
    .select("s.*", F.col("b.age"))
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

with_last = (
    latest
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.opened_date",
        "s.subscription_id",
        "s.transaction_merchant",
        "s.age",
        F.col("c.merchant").alias("transaction_merchant_2"),
    )
)

latest_match = (
    with_last
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant").alias("transaction_merchant_3"))
)

joined = (
    latest_match
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.age").alias("customer_age"))
)

result = (
    joined
    .select(
        "id",
        "customer_id",
        "opened_date",
        "subscription_id",
        "transaction_merchant",
        "transaction_merchant_2",
        "transaction_merchant_3",
        F.when(F.col("transaction_merchant_3").rlike("^(Grab|7-Eleven)$"), "GROUP_B").otherwise("CORE").alias("transaction_merchant_3_zone"),
    )
    .orderBy(F.col("transaction_merchant").desc_nulls_last())
)
