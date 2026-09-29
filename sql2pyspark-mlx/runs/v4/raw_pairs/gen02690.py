from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.sum(F.when(F.col("method") == "card", F.col("amount")).otherwise(0)).alias("card_amount"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("card_amount") < 43)
)
