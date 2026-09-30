from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "LOAN", "CREDIT"]) & F.col("opened_date").between("2024-03-30", "2024-09-01"))
    .withColumn("years_since_opened_date", (F.datediff(F.current_date(), "opened_date") / 365))
    .orderBy(F.col("customer_id").desc())
)
