from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Lazada") & (F.col("is_flagged") | F.col("account_id").isin([8, 20, 30])))
    .withColumn("txn_time_day", F.col("txn_time").cast("date"))
    .withColumn("amount_int", F.col("amount").cast("int"))
    .orderBy(F.col("txn_time").desc(), "id")
    .limit(10)
)
