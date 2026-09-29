from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") != "Shell") & F.col("merchant").isin(["Shell", "Amazon"]))
    .select("id", "is_flagged")
    .orderBy("id")
)
