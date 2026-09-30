from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isNotNull() & F.col("customer_id").isin([30, 31, 34]))
    .groupBy(
        "status",
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
    )
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.count("balance").alias("balance_count"),
    )
    .orderBy(F.col("status").desc(), "opened_cohort", "opened_month")
    .limit(10)
)
