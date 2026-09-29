from pyspark.sql import functions as F

filtered_accounts = df_accounts.filter(F.col("customer_id").isin([1, 34, 39]))

agg = (
    filtered_accounts
    .groupBy("account_type")
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0)).alias("frozen_count"),
    )
)

result = agg.select("account_type", "frozen_count")
