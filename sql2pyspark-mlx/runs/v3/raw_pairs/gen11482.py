from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") < 790) | F.col("method").isin(["paypal", "card", "cash"]))
    .groupBy("status")
    .agg(
        F.max("amount").alias("max_amount"),
        F.sum("amount").alias("total_amount"),
    )
)
