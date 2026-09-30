from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy("balance_int", "id", "subscription_id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("account_type").orderBy(F.col("monthly_fee").desc_nulls_last())

valid = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-08-26").cast("date")) & (F.col("balance") <= 38000))
    .withColumn("balance_int", F.col("balance").cast("int"))
)

matched = (
    valid
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.opened_date") >= F.col("sub.start_date")) & (F.col("a.opened_date") <= F.date_add(F.col("sub.end_date"), 10)), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_windows = (
    matched
    .filter(F.col("subscription_id") < 5)
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "subscription_id",
        F.min("monthly_fee").over(w).alias("last7_min_monthly_fee"),
        F.avg("monthly_fee").over(w).alias("last7_avg_monthly_fee"),
        F.sum("monthly_fee").over(w).alias("last7_sum_monthly_fee"),
        F.rank().over(w2).alias("monthly_fee_rank"),
    )
)

joined = (
    with_windows
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.name"), F.col("b.age"))
)

result = (
    joined
    .groupBy("customer_id")
    .agg(
        F.round(F.sum("subscription_id") / F.nullif(F.count(F.when(F.col("account_type") == "CHECKING", 1)), F.lit(0)), 2).alias("subscription_id_per_checking"),
        F.sum(F.when(F.col("account_type") == "SAVINGS", F.col("subscription_id"))).alias("savings_total_subscription_id"),
    )
    .orderBy(F.col("savings_total_subscription_id").desc())
)
