from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "CLOSED") & (F.col("balance") > 40000) & F.col("status").isin(["CLOSED", "FROZEN"]))
    .select(
        "id",
        "opened_date",
        F.when(F.col("balance") >= 71000, "High").when(F.col("balance") >= 9000, "Medium").otherwise("Low").alias("balance_tier"),
        F.substring("account_type", 1, 3).alias("account_type_prefix"),
    )
)
