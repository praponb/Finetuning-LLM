from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").asc_nulls_first())
w2 = Window.partitionBy("txn_type", "account_type").orderBy(F.col("balance").asc_nulls_first(), "id")

valid = (
    df_transactions
    .filter(~F.col("merchant").eqNullSafe(F.lit("Shell")))
    .withColumn("txn_time_day", F.col("txn_time").cast("date"))
)

ranked = (
    valid
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

joined = (
    ranked
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.balance"), F.col("b.account_type"), F.col("b.opened_date"))
)

ranked_transactions = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked_transactions
    .groupBy(
        F.when(F.col("balance") >= 87000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
        "merchant",
    )
    .agg(
        F.percentile_approx("balance", 0.9).alias("balance_p90"),
        F.max("txn_time").alias("last_txn_time"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
    )
    .orderBy(F.col("merchant").asc_nulls_last())
)
