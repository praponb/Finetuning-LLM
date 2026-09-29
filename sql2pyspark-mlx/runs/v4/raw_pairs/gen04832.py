from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("activity_month", F.date_trunc("MONTH", "opened_date"))
    .rollup("activity_month", "customer_id")
    .agg(
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.sum("balance").alias("total_balance"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy("avg_balance")
)
