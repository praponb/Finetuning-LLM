from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("shipping_fee").alias("sum_order_shipping_fee"))

enriched = (
    df_accounts
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.signup_date"), F.col("c.segment"))
)

enriched_accounts = (
    enriched
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .select("e.*", F.col("c.credit_limit"), F.col("c.name"), F.col("c.city"))
)

attributed = (
    enriched_accounts
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.signup_date") >= F.col("sub.start_date")) & (F.col("e.signup_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "e.*",
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
    .select("s.*", F.col("c.amount"), F.col("c.txn_time").alias("transaction_txn_time"))
)

result = (
    with_latest
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "signup_date",
        "credit_limit",
        "name",
        F.col("sum_order_shipping_fee"),
    )
    .orderBy(F.col("status").desc_nulls_first(), "credit_limit")
)
