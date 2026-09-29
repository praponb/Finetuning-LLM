from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(12, 19))
    .groupBy("method")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.sum(F.when(F.col("method") == "card", 1).otherwise(0)).alias("card_count"),
    )
    .filter(F.col("card_count") < 49)
    .orderBy(F.col("total_amount").desc())
)
