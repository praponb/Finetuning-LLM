from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([8, 9, 23]) & ((F.col("account_type") == "LOAN") | F.col("account_type").isin(["SAVINGS", "LOAN", "CHECKING"])))
    .groupBy(
        F.when(F.col("status").isin(["OPEN", "CLOSED"]), "core").otherwise("other").alias("status_group"),
        "customer_id",
    )
    .agg(
        F.count("*").alias("total_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_frozen"),
    )
)
