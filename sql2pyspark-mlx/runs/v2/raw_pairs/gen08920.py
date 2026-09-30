from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .withColumn("opened_date_weekofyear", F.weekofyear("opened_date"))
)

summary = (
    base
    .groupBy("account_type", F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.round(((100 * F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_savings"),
    )
)

result = summary.select("account_type", "avg_balance", "pct_savings")
