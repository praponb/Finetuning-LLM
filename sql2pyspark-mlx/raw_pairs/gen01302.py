from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("balance").asc_nulls_last())
w2 = Window.partitionBy("customer_id").orderBy("balance", "id")
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id")
w_grp = Window.partitionBy("account_type", "customer_id").orderBy("subscription_id", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_accounts
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    ranked
    .filter(F.col("balance") <= 21000)
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("cumulative_balance", F.sum("balance").over(w3))
    .withColumn("customer_id_max_balance", F.max("balance").over(w4))
)

attributed = (
    scored
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.opened_date") >= F.col("sub.start_date")) & (F.col("e.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

eligible = (
    attributed
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.merchant").isin(["7-Eleven", "Amazon", "Shell"])), "left_anti")
)

enriched = (
    eligible
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "row_num",
        "subscription_id",
        F.avg("cumulative_balance").over(w_grp).alias("running_avg_cumulative_balance"),
        F.max("cumulative_balance").over(w_grp).alias("running_max_cumulative_balance"),
        F.min("cumulative_balance").over(w_grp).alias("running_min_cumulative_balance"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("running_max_cumulative_balance") >= 382000, "HIGH").otherwise("LOW").alias("running_max_cumulative_balance_tier"),
    )
    .agg(
        F.round(F.avg("running_min_cumulative_balance"), 2).alias("avg_running_min_cumulative_balance"),
        F.percentile_approx("subscription_id", 0.95).alias("subscription_id_p95"),
        F.min("running_min_cumulative_balance").alias("min_running_min_cumulative_balance"),
    )
    .filter(F.col("subscription_id_p95") < 1000000)
    .orderBy(F.col("avg_running_min_cumulative_balance").asc_nulls_last())
)
