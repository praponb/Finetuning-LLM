from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "CHECKING")
    .select(
        "balance",
        F.when(F.col("account_type").isin(["CREDIT", "LOAN"]), "Priority").otherwise("Other").alias("account_type_group"),
    )
    .orderBy("balance")
)
