from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(900, 1210))
    .groupBy("status")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count(F.when(F.col("method") == "card", 1)).alias("card_count"),
    )
    .filter(F.col("card_count") > 3)
    .select("status", "total_amount", "card_count")
)
