from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") | F.col("amount").between(2100, 3000))
    .groupBy(
        F.when(F.col("merchant").isin(["Starbucks", "Amazon"]), "group_a").otherwise("other").alias("merchant_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0))) / F.count("*")), 1).alias("pct_7_eleven"),
        F.max("amount").alias("max_amount"),
        F.sum("amount").alias("total_amount"),
    )
    .orderBy(F.col("total_amount").desc())
)
