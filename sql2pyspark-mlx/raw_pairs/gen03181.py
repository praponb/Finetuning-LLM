from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("customer_id") == 37) & (F.col("opened_date") < "2023-04-01"))
    .agg(F.sort_array(F.collect_set("account_type")).alias("account_type_list"))
)
