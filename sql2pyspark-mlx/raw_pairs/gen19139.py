from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "LOAN"]))
    .select(
        "id",
        "customer_id",
        "status",
        "account_type",
        F.when(F.col("account_type").isin(["CHECKING", "SAVINGS"]), "Tier 1").otherwise("Other").alias("account_type_group"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
