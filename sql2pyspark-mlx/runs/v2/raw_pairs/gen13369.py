from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isNotNull() & (F.col("balance") < 55000) & (F.col("balance") >= 47000))
    .agg(
        F.sum("balance").alias("total_balance"),
        F.round(((100 * F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_frozen"),
    )
    .orderBy("pct_frozen")
)
