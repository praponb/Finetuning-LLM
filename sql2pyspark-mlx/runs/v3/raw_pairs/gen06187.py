from pyspark.sql import functions as F

result = (
    df_accounts
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0)).alias("frozen_count"),
    )
    .orderBy(F.col("frozen_count").desc())
)
