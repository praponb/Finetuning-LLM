from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        "status",
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
    )
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
    .orderBy("opened_cohort")
)
