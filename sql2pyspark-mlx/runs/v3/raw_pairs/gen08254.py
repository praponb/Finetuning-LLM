from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 6)
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
    )
    .filter(F.col("balance_range") > 38)
    .orderBy(F.col("opened_month").desc())
)
