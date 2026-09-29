from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") & ((F.to_date("txn_time") < "2025-08-01") | F.col("merchant").isin(["Lazada", "Shell", "7-Eleven"])))
    .select("id", "txn_time", F.upper("merchant").alias("merchant_upper"))
    .orderBy(F.col("merchant_upper").desc())
)
