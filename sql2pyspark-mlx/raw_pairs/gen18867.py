from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "LOAN", "CREDIT"]))
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
    )
    .filter(F.col("balance_range") >= 55)
    .orderBy(F.col("opened_month").desc())
)
