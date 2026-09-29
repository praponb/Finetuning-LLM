from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.sum("balance").alias("total_balance"),
        F.avg("balance").alias("avg_balance"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
    .filter(F.col("total_balance") < 373000)
)
