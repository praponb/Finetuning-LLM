from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("status").orderBy("balance", "id")
w2 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_accounts
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-06-04"))) & (~F.col("status").eqNullSafe(F.lit("CLOSED"))))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        F.date_trunc("MONTH", "opened_date").alias("opened_date_period"),
    )
)

enriched = (
    filtered
    .filter(~F.col("status").isin(["CLOSED", "OPEN"]))
    .select(
        "id",
        "balance",
        "status",
        "opened_date_period",
        F.lag("balance").over(w_part).alias("prev_balance"),
        F.row_number().over(w_part).alias("event_seq"),
        F.first("status", ignorenulls=True).over(w2).alias("first_known_status"),
    )
)

remaining_keys = (
    enriched
    .select("status", "id")
    .exceptAll(
        df_accounts
        .filter(F.col("customer_id").isin([14, 27, 28, 29]))
        .select("status", "id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date_period")), "left_anti")
)

result = (
    eligible
    .withColumn("week_start", F.date_trunc("WEEK", "opened_date_period"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "opened_date_period"))
    .groupingSets(
        [["week_start", "quarter_start"], ["week_start"], []],
        "week_start",
        "quarter_start",
    )
    .agg(
        F.percentile_approx("prev_balance", 0.5).alias("prev_balance_p50"),
        F.sum(F.when(F.col("status") == "CLOSED", F.col("prev_balance"))).alias("closed_total_prev_balance"),
        F.count_if(F.col("status") == "OPEN").alias("open_events"),
        F.stddev_samp("balance").alias("balance_stddev"),
        F.grouping_id().alias("gid"),
    )
)
