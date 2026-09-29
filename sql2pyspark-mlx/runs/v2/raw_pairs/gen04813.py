from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
)
