from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-11-20", "2025-01-29"))
    .withColumn("days_since_opened_date", F.datediff(F.current_date(), "opened_date"))
)
