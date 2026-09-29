from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .groupBy("status")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
    )
)
