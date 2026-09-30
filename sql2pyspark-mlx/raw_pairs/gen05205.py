from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & (F.col("amount") > 1700) & F.col("amount").between(3000, 3200))
    .groupBy(F.year("txn_time").alias("txn_year"))
    .agg(
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
    )
)
