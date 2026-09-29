from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") != "Shell") & (F.col("amount") > 3000) & F.col("merchant").isin(["Starbucks", "Amazon", "Lazada"]))
    .select("id", "account_id")
)
