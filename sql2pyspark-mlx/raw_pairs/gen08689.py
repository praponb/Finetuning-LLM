from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([6, 10, 21]) | (F.col("customer_id") == 9))
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"))
    .orderBy("savings_count", "opened_month", "opened_cohort")
    .limit(20)
)
