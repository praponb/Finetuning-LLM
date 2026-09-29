from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "opened_date",
        F.when(F.col("account_type").isin(["SAVINGS", "CREDIT"]), "Tier 1").otherwise("Other").alias("account_type_group"),
        F.substring("account_type", 1, 2).alias("account_type_prefix"),
    )
    .orderBy("opened_date")
)
