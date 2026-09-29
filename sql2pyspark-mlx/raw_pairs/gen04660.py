from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter(F.month("opened_date").isin([5, 10, 11]) & ((F.col("account_type") == "SAVINGS") | F.col("status").isNotNull()))
)

agg = (
    valid_accounts
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0)).alias("frozen_count"),
    )
)

result = (
    agg
    .filter(F.col("frozen_count") < 25)
    .select("status", "frozen_count")
    .orderBy(F.col("status").desc())
    .limit(20)
)
