from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Grab", "Shell", "Lazada"]))
    .select("id", "is_flagged")
)
