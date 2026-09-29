from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "OPEN", "FROZEN"]))
    .groupBy(
        "account_type",
        "status",
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(F.round(F.stddev("balance"), 2).alias("stddev_balance"))
)
