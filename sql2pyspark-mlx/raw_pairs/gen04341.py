from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").asc_nulls_first(), "id")

latest = (
    df_transactions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "account_id", "amount", "txn_time")
)

result = (
    latest
    .groupBy("account_id")
    .agg(
        F.max("txn_time").alias("ended_at"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.min("txn_time").alias("first_seen"),
    )
    .orderBy(F.col("account_id").desc())
)
