from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.count("*").alias("num_accounts"),
    )
    .orderBy(F.col("num_accounts").desc())
)
