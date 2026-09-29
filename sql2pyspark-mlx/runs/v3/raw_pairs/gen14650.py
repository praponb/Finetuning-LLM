from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("account_type").isin(["SAVINGS", "CHECKING"]), "tier_1").otherwise("other").alias("account_type_group"),
    )
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
    .filter(F.col("balance_range") >= 61)
    .select("account_type_group", "balance_range")
    .orderBy(F.col("account_type_group").desc(), F.col("balance_range").desc())
)
