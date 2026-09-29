from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") != "CREDIT")
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.max("balance").alias("max_balance"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") > 4)
)
