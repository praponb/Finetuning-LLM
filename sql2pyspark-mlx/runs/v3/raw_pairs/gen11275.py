from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(F.sort_array(F.collect_set("account_type")).alias("account_type_list"))
)
