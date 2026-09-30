from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "OPEN")
    .select(
        "id",
        "account_type",
        "status",
        F.when(F.col("account_type").isin(["CREDIT", "CHECKING"]), "Priority").otherwise("Other").alias("account_type_group"),
    )
    .orderBy(F.col("account_type_group").desc(), "id")
    .limit(5)
)
