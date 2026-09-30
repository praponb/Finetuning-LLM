from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "opened_date",
        "customer_id",
        "balance",
        F.when(F.col("balance") >= 65000, "High").when(F.col("balance") >= 12000, "Medium").otherwise("Low").alias("balance_band"),
    )
    .orderBy("id")
    .limit(1)
)
