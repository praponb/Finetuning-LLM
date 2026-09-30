from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "SAVINGS"]))
    .withColumn("status_prefix", F.substring("status", 1, 4))
)

summary = (
    base
    .groupBy("status")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.count("*").alias("total_count"),
    )
)

result = summary.filter(F.col("total_count") >= 7)
