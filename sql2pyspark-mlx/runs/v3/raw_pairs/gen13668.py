from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "FROZEN", "CLOSED"]) & ~F.col("account_type").isin(["CHECKING", "SAVINGS"]))
    .groupBy(
        "status",
        F.when(F.col("account_type").isin(["LOAN", "CREDIT"]), "tier_1").otherwise("other").alias("account_type_group"),
        F.when(F.col("balance") >= 67000, "high").when(F.col("balance") >= 33000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.min("opened_date").alias("first_opened_date"))
    .orderBy("first_opened_date")
)
