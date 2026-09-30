from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(((F.col("balance") < 64000) & (F.col("account_type") == "CHECKING")) | (F.col("opened_date") < "2025-08-31"))
    .groupBy(
        "account_type",
        F.when(F.col("status").isin(["OPEN", "FROZEN"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0)).alias("open_count"),
        F.count("*").alias("account_count"),
    )
)
