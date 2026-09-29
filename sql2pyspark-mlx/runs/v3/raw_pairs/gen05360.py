from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count(F.when(F.col("merchant") == "Lazada", 1)).alias("lazada_count"),
    )
)
