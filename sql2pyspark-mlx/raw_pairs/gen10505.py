from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0))) / F.count("*")), 1).alias("pct_7_eleven"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.min("amount").alias("min_amount"),
    )
    .orderBy("pct_7_eleven")
)
