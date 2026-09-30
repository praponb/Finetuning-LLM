from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("opened_date") < "2024-12-01")
    .withColumn("opened_date_plus_63d", F.date_add("opened_date", 63))
)

agg = (
    base
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        "account_type",
    )
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
    )
)

result = agg.select("opened_cohort", "account_type", "frozen_balance")
