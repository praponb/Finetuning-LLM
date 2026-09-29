from pyspark.sql import functions as F

result = (
    df_accounts
    .agg(
        F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0)).alias("savings_count"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.countDistinct("balance").alias("unique_statuss"),
    )
    .orderBy("unique_statuss", "savings_count")
)
