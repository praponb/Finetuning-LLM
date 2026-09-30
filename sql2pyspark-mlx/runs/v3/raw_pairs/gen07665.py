from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") == "FEE") & F.col("amount").between(3400, 3700))
    .agg(
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("credit_amount"),
    )
)
