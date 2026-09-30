from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2025-04-05", "2025-06-01"))
    .select(
        "id",
        "opened_date",
        F.when(F.col("balance") >= 82000, "High").when(F.col("balance") >= 22000, "Medium").otherwise("Low").alias("balance_band"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
