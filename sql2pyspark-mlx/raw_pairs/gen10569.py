from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") != "OPEN") & F.col("opened_date").between("2023-07-01", "2025-05-01") & F.month("opened_date").isin([5, 9, 11]))
    .select("opened_date")
)
