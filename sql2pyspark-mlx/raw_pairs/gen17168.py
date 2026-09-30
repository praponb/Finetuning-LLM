from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~((F.col("balance") >= 18000) & F.col("customer_id").isin([11, 14, 27])))
    .groupBy(
        "account_type",
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.sum("balance").alias("total_balance"),
        F.countDistinct("balance").alias("unique_account_types"),
    )
    .orderBy("account_type", "opened_cohort")
    .limit(5)
)
