from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .pivot("merchant", ["7-Eleven", "Shell"])
    .agg(F.avg("amount"))
)
