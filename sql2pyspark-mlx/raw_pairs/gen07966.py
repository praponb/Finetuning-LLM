from pyspark.sql import functions as F

src = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "CREDIT", "LOAN"]))
    .withColumn("status_account_type", F.concat("status", F.lit(", "), "account_type"))
)

agg = (
    src
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.min("balance").alias("min_balance"),
    )
)

result = (
    agg
    .select("opened_month", "stddev_balance")
    .orderBy(F.col("opened_month").desc())
)
