from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id", F.month("opened_date").alias("opened_month"))
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.count("*").alias("num_accounts"),
    )
    .orderBy("opened_month")
)
