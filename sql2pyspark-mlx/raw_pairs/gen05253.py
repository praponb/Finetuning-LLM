from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc(), "id")

cleaned = (
    df_transactions
    .filter((~F.col("merchant").eqNullSafe(F.lit("Shell"))) & (F.col("amount") < 3900))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.lower("merchant").alias("merchant_lower"),
        F.dayofweek("txn_time").alias("txn_time_dayofweek"),
        F.coalesce("merchant", F.lit("NONE")).alias("resolved_merchant"),
    )
)

result = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
