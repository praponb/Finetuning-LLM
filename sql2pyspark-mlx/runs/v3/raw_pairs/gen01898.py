from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["CLOSED", "OPEN"]))
    .groupBy(
        F.month("opened_date").alias("opened_month"),
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        F.when(F.col("balance") >= 88000, "high").when(F.col("balance") >= 26000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.min("opened_date").alias("min_opened_date"),
        F.max("opened_date").alias("latest_opened_date"),
    )
    .orderBy("latest_opened_date")
)
