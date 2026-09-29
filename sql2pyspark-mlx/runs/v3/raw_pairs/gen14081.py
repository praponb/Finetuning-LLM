from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter(~((F.col("status") == "CLOSED") & (F.col("opened_date") <= "2024-12-23")))
    .withColumn("balance_rounded", F.round("balance", 0))
)

accounts_stats = (
    valid_accounts
    .groupBy(
        F.month("opened_date").alias("opened_month"),
        F.when(F.col("account_type").isin(["CHECKING", "SAVINGS"]), "core").otherwise("other").alias("account_type_group"),
    )
    .agg(
        F.min("balance_rounded").alias("min_balance_rounded"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
)

result = accounts_stats
