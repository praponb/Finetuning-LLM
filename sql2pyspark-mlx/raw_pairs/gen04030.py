from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id").orderBy(F.col("years_since_txn_time").asc_nulls_first(), "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time_dayofweek", "id")

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & F.col("merchant").isin(["Starbucks", "Grab", "Lazada"]))
    .select(
        "*",
        F.dayofweek("txn_time").alias("txn_time_dayofweek"),
        (F.datediff(F.current_date(), "txn_time") / 365.25).alias("years_since_txn_time"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_filled"),
    )
)

deduped = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    deduped
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.account_id",
        "a.txn_type",
        "a.amount",
        "a.merchant",
        "a.is_flagged",
        "a.txn_time_dayofweek",
        "a.years_since_txn_time",
        F.col("p.opened_date"),
        F.col("p.status"),
    )
)

latest = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("YEAR", "opened_date").alias("year_start"))
    .agg(
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.round(F.sum("txn_time_dayofweek") / F.nullif(F.count(F.when(F.col("status") == "CLOSED", 1)), F.lit(0)), 2).alias("txn_time_dayofweek_per_closed"),
        F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_count"),
    )
    .orderBy(F.col("year_start").asc_nulls_first())
)
