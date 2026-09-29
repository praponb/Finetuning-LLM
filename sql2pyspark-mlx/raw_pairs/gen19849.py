from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") > 33000)
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(F.countDistinct("balance").alias("unique_statuss"))
    .orderBy(F.col("opened_month").desc(), "unique_statuss")
)
