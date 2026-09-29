from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "CLOSED"]) | (F.col("status") != "CLOSED"))
    .select(
        "id",
        "account_type",
        "status",
        F.when(F.col("balance") >= 62000, "High").when(F.col("balance") >= 25000, "Medium").otherwise("Low").alias("balance_level"),
    )
    .orderBy("id")
)
