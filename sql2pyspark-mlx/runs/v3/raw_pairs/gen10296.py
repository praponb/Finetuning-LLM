from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("account_type").isin(["CHECKING", "LOAN"]), "priority").otherwise("other").alias("account_type_group"),
    )
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.max("balance").alias("max_balance"),
    )
)
