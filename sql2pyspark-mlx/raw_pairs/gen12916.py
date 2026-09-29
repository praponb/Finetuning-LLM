from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") >= 57000) | F.month("opened_date").isin([8, 9, 12]))
    .groupBy(
        "status",
        F.when(F.col("account_type").isin(["CREDIT", "CHECKING"]), "tier_1").otherwise("other").alias("account_type_group"),
    )
    .agg(
        F.count("*").alias("num_accounts"),
        F.sum("balance").alias("total_balance"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .orderBy(F.col("min_opened_date").desc())
)
