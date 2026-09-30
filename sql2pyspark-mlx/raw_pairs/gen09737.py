from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .pivot("status", ["FROZEN", "OPEN"])
    .agg(F.max("balance").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "account_type",
        F.col("FROZEN_total").alias("frozen_total"),
        F.col("FROZEN_cnt").alias("frozen_cnt"),
        F.col("OPEN_total").alias("open_total"),
        F.col("OPEN_cnt").alias("open_cnt"),
    )
)
