from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Grab") & ((F.to_date("txn_time") < "2024-11-01") | F.col("merchant").isin(["Starbucks", "Grab", "7-Eleven"])))
    .select("is_flagged")
)
