from pyspark.sql import functions as F

result = (
    df_accounts
    .rollup(
        F.month("opened_date").alias("opened_month"),
        F.year("opened_date").alias("opened_year"),
    )
    .agg(F.sum("balance").alias("total_balance"))
)
