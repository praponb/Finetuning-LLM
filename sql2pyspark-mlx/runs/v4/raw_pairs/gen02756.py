from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.stddev_samp("balance").alias("balance_stddev"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
)
