from pyspark.sql import functions as F

filtered_accounts = df_accounts.filter(F.col("balance") > 76000)

accounts_stats = (
    filtered_accounts
    .groupBy("account_type")
    .agg(
        F.countDistinct("balance").alias("unique_statuss"),
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
    )
)

result = accounts_stats.select("account_type", "account_type_values")
