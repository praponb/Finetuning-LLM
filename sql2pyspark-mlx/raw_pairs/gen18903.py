from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .groupBy("account_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_frozen"),
    )
    .filter(F.col("pct_frozen") >= 34)
)
