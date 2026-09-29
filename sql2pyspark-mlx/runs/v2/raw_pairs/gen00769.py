from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "SAVINGS") & F.col("opened_date").between("2024-11-01", "2024-12-01") & (F.col("status") == "CLOSED"))
    .agg(
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
        F.count("*").alias("account_count"),
    )
    .orderBy("open_count")
)
