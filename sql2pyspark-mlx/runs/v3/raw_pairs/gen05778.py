from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("txn_time") < "2024-11-24")
    .withColumn("txn_type_merchant", F.concat("txn_type", F.lit(", "), "merchant"))
)

summary = (
    base
    .groupBy(F.to_date("txn_time").alias("txn_day"))
    .agg(
        F.countDistinct("amount").alias("unique_accounts"),
        F.count("*").alias("total_count"),
    )
)

result = summary.filter(F.col("total_count") >= 5)
