from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
        F.when(F.col("balance") >= 70000, "high").when(F.col("balance") >= 16000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.countDistinct("balance").alias("unique_statuss"),
    )
    .filter(F.col("unique_statuss") > 8)
)
