from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([3, 6, 11]) & (F.col("opened_date") > "2025-04-06"))
    .select("opened_date")
    .orderBy(F.col("opened_date").desc())
)
