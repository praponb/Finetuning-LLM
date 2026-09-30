from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "opened_date",
        "account_type",
        F.col("balance").cast("int").alias("balance_int"),
        F.when(F.col("balance") >= 79000, "High").when(F.col("balance") >= 15000, "Medium").otherwise("Low").alias("balance_band"),
    )
    .orderBy("account_type", F.col("balance_band").desc())
)
