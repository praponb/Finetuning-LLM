from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "SAVINGS") & ((F.col("balance") > 62000) | (F.col("status") == "OPEN")))
    .select(
        "id",
        "balance",
        F.when(F.col("account_type").isin(["SAVINGS", "CREDIT"]), "Group A").otherwise("Other").alias("account_type_group"),
    )
    .orderBy("balance")
)
