from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.to_date("txn_time") < "2024-08-01") & F.col("is_flagged"))
    .groupBy("is_flagged")
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_count"),
    )
    .filter(F.col("amazon_count") > 3)
    .orderBy(F.col("cnt").desc())
)
