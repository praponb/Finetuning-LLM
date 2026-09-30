from pyspark.sql import functions as F

result = (
    df_transactions
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.min("amount").alias("min_amount"),
    )
    .orderBy(F.col("min_amount").desc())
)
