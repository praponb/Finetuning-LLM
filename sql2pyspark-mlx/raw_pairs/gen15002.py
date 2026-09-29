from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("merchant").isin(["Amazon", "7-Eleven"]), "group_a").otherwise("other").alias("merchant_group"),
    )
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") < 20000)
    .select("merchant_group", "total_amount")
)
