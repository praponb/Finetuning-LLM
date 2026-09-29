from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("account_type") == "CHECKING", 1).otherwise(0)).alias("checking_count"),
    )
    .orderBy("opened_cohort")
)
