from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

not_flagged = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.amount").between(2200, 2900)), "left_anti")
)

filtered = (
    not_flagged
    .filter((F.col("opened_date") > F.lit("2024-04-30").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.round("balance", 0).alias("balance_rounded"),
        F.round((F.col("balance") * 1.1), 2).alias("adjusted_balance"),
    )
)

latest_match = (
    filtered
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant"))
)

joined = (
    latest_match
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select("src.*", F.col("c.country"), F.col("c.credit_limit"), F.col("c.email"))
)

matched = (
    joined
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.opened_date") >= F.col("sub.start_date")) & (F.col("s.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    matched
    .groupBy("status")
    .pivot("country", ["TH", "SG"])
    .agg(F.avg("balance_rounded").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
