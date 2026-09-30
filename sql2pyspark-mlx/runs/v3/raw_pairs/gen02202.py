from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") <= "2023-08-01")
    .groupBy("account_type", "status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.avg("balance").alias("avg_balance"),
    )
    .orderBy("account_type")
)
