from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") != "FEE") & (F.col("amount") > 3000) & F.col("amount").between(1100, 1200))
    .select("id", "txn_type")
)
