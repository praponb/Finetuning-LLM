from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~((F.col("opened_date") >= "2023-06-21") & (F.col("account_type") != "LOAN") & F.month("opened_date").isin([3, 4, 8])))
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
    )
    .orderBy("min_opened_date")
)
