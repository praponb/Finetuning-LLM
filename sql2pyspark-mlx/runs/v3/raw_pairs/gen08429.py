from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "LOAN") & (F.col("opened_date") <= "2024-02-01"))
    .select(
        "opened_date",
        "balance",
        "account_type",
        F.when(F.col("account_type").isin(["CHECKING", "LOAN"]), "Group A").otherwise("Other").alias("account_type_group"),
        F.when(F.col("balance") >= 66000, "High").when(F.col("balance") >= 30000, "Medium").otherwise("Low").alias("balance_level"),
    )
)
