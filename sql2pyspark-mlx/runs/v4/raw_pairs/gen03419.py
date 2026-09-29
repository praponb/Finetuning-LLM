from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("balance").desc_nulls_first())
w3 = Window.orderBy(F.col("balance").desc(), "id")
w4 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("account_type").orderBy(F.col("status_length").asc_nulls_first())
w6 = Window.partitionBy("status").orderBy("cumulative_status_length", "id")
w7 = Window.partitionBy("account_type").orderBy("balance", "id")
w8 = Window.partitionBy("activity_month").orderBy(F.col("open_events").desc())

base = (
    df_accounts
    .filter(F.col("status").isNotNull() & (F.col("account_type") == "SAVINGS"))
    .select(
        "*",
        F.add_months("opened_date", 6).alias("opened_date_plus_months"),
        F.coalesce("status", F.lit("OTHER")).alias("status_filled"),
    )
)

valid = (
    base
    .filter((F.col("opened_date") >= F.lit("2023-10-05").cast("date")) & F.col("account_type").isin(["CHECKING", "SAVINGS"]))
    .withColumn("status_length", F.length("status"))
    .withColumn("status_filled_2", F.coalesce("status", F.lit("OTHER")))
)

with_windows = (
    valid
    .select(
        "*",
        F.first("balance", ignorenulls=True).over(w).alias("first_known_balance"),
        F.dense_rank().over(w2).alias("balance_dense_rank"),
        F.ntile(3).over(w3).alias("balance_bucket"),
        F.sum("status_length").over(w4).alias("cumulative_status_length"),
        F.dense_rank().over(w5).alias("status_length_dense_rank"),
    )
)

metrics = (
    with_windows
    .withColumn("cumulative_status_length_bucket", F.ntile(3).over(w6))
    .withColumn("balance_bucket_2", F.ntile(4).over(w7))
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date_plus_months")), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("activity_month"),
        F.date_trunc("QUARTER", "opened_date_plus_months").alias("quarter_start"),
    )
    .agg(
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_rows"),
        F.count_if(F.col("status") == "OPEN").alias("open_events"),
        F.sum("status_length").alias("total_status_length"),
    )
    .withColumn("open_events_dense_rank", F.dense_rank().over(w8))
    .orderBy(F.col("open_events").asc_nulls_last())
)
