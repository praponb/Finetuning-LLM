from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.year("opened_date") >= 2025) | ~F.col("account_type").isin(["SAVINGS", "LOAN"]))
    .select(
        "id",
        "customer_id",
        F.when(F.col("account_type").isin(["CHECKING", "CREDIT"]), "Tier 1").otherwise("Other").alias("account_type_group"),
    )
)
