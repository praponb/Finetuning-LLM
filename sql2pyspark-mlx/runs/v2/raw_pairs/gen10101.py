from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(51000, 54000))
    .groupBy("status")
    .agg(F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"))
)
