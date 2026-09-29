from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "SAVINGS")
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        "account_type",
    )
    .agg(F.count("*").alias("cnt"))
)
