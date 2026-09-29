from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").isNotNull())
    .groupBy("status")
    .agg(
        F.countDistinct("balance").alias("unique_statuss"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
    .orderBy(F.col("unique_statuss").desc())
)
