from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.stddev_samp("balance").alias("balance_stddev"),
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list_2"),
        F.avg(F.when(F.col("status") == "FROZEN", F.col("balance"))).alias("frozen_avg_balance"),
    )
    .filter(F.col("frozen_avg_balance") < 1000000)
    .orderBy("num_status")
)
