from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .pivot("merchant", ["Starbucks", "Grab", "Lazada"])
    .agg(F.sum("amount"))
    .orderBy("is_flagged")
)
