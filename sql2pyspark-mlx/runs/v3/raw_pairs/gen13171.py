from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 24000)
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(F.min("balance").alias("min_balance"))
)
