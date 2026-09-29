from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", F.year("opened_date").alias("opened_year"), "account_type")
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.min("opened_date").alias("first_opened_date"),
    )
    .orderBy("status", "opened_year", "account_type")
    .limit(10)
)
