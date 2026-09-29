from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .groupBy("account_type")
    .agg(
        F.min("balance").alias("min_balance"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
    )
    .filter(F.col("min_balance") >= 18000)
    .orderBy("min_balance", "account_type")
    .limit(50)
)
