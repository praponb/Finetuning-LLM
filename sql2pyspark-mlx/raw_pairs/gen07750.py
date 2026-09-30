from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["OPEN", "FROZEN"]) & (F.col("account_type") == "SAVINGS"))
    .select(
        "account_type",
        "balance",
        "opened_date",
        F.when(F.col("balance") >= 78000, "High").when(F.col("balance") >= 26000, "Medium").otherwise("Low").alias("balance_level"),
        F.substring("account_type", 1, 4).alias("account_type_prefix"),
    )
    .orderBy(F.col("account_type_prefix").desc())
)
