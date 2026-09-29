from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(11, 16))
    .groupBy("merchant")
    .agg(
        F.max("amount").alias("max_amount"),
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
    )
    .filter(F.col("pct_credit") > 62)
    .orderBy(F.col("max_amount").desc())
)
