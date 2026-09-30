from pyspark.sql import functions as F

base = df_accounts.filter(F.year("opened_date") >= 2024)

summary = (
    base
    .groupBy("status")
    .agg(F.round(F.stddev("balance"), 2).alias("stddev_balance"))
)

result = summary.filter(F.col("stddev_balance") < 66)
