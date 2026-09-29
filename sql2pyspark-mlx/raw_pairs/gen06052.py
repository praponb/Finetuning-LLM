from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "SAVINGS", "CHECKING"]))
    .groupBy("customer_id")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.min("balance").alias("min_balance"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .orderBy("stddev_balance", "customer_id")
    .limit(1)
)
