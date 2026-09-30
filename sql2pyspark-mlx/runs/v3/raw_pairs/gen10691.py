from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.sum(F.when(F.col("status") == "OPEN", F.col("balance")).otherwise(0)).alias("open_balance"),
        F.countDistinct("balance").alias("unique_account_types"),
    )
    .filter(F.col("unique_account_types") > 2)
    .orderBy(F.col("open_balance").desc())
)
