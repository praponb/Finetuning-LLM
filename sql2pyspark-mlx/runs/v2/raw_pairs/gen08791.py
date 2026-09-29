from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") & (F.col("merchant") == "Shell"))
    .groupBy("is_flagged")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") < 3800)
)
