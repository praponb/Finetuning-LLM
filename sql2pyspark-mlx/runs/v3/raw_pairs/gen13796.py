from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "7-Eleven") & (F.col("merchant").isNull() | (F.col("amount") > 1500)))
    .groupBy("merchant")
    .agg(
        F.max("amount").alias("max_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
    .orderBy("merchant")
)
