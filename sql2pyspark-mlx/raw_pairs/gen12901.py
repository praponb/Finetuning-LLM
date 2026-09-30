from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
        F.max("opened_date").alias("last_opened_date"),
    )
    .filter(F.col("frozen_count") < 4)
    .orderBy(F.col("customer_id").desc())
)
