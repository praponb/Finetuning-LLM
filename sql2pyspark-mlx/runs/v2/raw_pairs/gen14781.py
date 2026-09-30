from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & (F.col("amount") >= 3500) & (F.to_date("txn_time") >= "2023-09-01"))
    .withColumn("txn_type_title", F.initcap(F.trim("txn_type")))
)
