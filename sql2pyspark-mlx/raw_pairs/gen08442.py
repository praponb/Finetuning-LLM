from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") < "2024-04-01")
    .select(
        "id",
        "status",
        "balance",
        "customer_id",
        F.round((F.col("balance") * 0.9), 2).alias("balance_scaled"),
        F.when(F.col("balance") >= 58000, "High").when(F.col("balance") >= 27000, "Medium").otherwise("Low").alias("balance_tier"),
    )
    .orderBy("balance_tier")
)
