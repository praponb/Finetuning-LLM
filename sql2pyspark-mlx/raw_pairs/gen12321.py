from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("status_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w_grp = Window.partitionBy("status").orderBy("status_length", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("account_type", "merchant").orderBy(F.col("transaction_amount").asc_nulls_last(), "id")

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-04-14").cast("date")) & F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.length("status").alias("status_length"),
        F.coalesce("status", F.lit("N/A")).alias("status_filled"),
    )
)

scored = (
    cleaned
    .filter(F.col("customer_id").isin([22, 30, 39]))
    .select(
        "*",
        F.sum("balance").over(w).alias("cumulative_balance"),
        F.avg("status_length").over(w2).alias("status_length_avg_6"),
        F.avg("status_length").over(w_grp).alias("status_length_avg_2"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant"), F.col("c.amount").alias("transaction_amount"))
)

deduped = (
    with_latest
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "status_length",
        "status_filled",
        "cumulative_balance",
        "status_length_avg_6",
    )
)

summary = (
    deduped
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_rows"),
        F.percentile_approx("status_length_avg_6", 0.5).alias("status_length_avg_6_p50"),
        F.count_if(F.col("account_type") == "CHECKING").alias("checking_events"),
    )
)

result = (
    summary
    .select("status", "closed_rows", "status_length_avg_6_p50", "checking_events")
    .orderBy("status")
    .limit(20)
)
