from pyspark.sql import functions as F

src = df_accounts.filter(F.col("account_type") != "SAVINGS")

summary = (
    src
    .groupBy(
        "status",
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.countDistinct("balance").alias("unique_statuss"),
    )
)

result = (
    summary
    .filter(F.col("unique_statuss") > 4)
    .select("status", "total_count", "unique_statuss")
)
