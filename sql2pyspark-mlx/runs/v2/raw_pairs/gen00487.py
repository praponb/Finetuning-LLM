from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance").between(18000, 75000) & F.col("balance").between(41000, 47000)) | (F.col("customer_id") == 27))
    .select(
        "id",
        "balance",
        "opened_date",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
        F.when(F.col("balance") >= 88000, "High").when(F.col("balance") >= 23000, "Medium").otherwise("Low").alias("balance_band"),
    )
)
