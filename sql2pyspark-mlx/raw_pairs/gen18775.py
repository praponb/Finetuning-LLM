from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id", "account_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("txn_time").desc(), "id")
w5 = Window.partitionBy(F.year("txn_time"), "account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("cumulative_event_seq_tier").orderBy(F.col("min_event_seq_2").desc())

with_dims = (
    df_transactions
    .alias("src")
    .join(df_accounts.alias("p"), F.col("src.account_id") == F.col("p.id"), "left")
    .select("src.id", "src.account_id", "src.txn_time", F.col("p.account_type"))
)

with_windows = (
    with_dims
    .select(
        "id",
        "account_id",
        F.row_number().over(w).alias("event_seq"),
        F.first("account_type", ignorenulls=True).over(w2).alias("first_known_account_type"),
        F.last("account_type", ignorenulls=True).over(w3).alias("last_known_account_type"),
        F.row_number().over(w4).alias("event_seq_2"),
        F.first("account_type", ignorenulls=True).over(w5).alias("first_known_account_type_2"),
    )
)

scored = (
    with_windows
    .filter(F.col("event_seq") < 4)
    .withColumn("cumulative_event_seq", F.sum("event_seq").over(w_ordered))
    .withColumn("last_known_event_seq", F.last("event_seq", ignorenulls=True).over(w_ordered))
)

valid = (
    scored
    .filter(F.col("event_seq_2") != 7)
    .select(
        "*",
        F.round((F.col("last_known_event_seq") * 100), 2).alias("adjusted_last_known_event_seq"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("cumulative_event_seq") >= 39, "LARGE").otherwise("SMALL").alias("cumulative_event_seq_tier"),
        F.when(F.col("event_seq") >= 8, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
    )
    .agg(
        F.round(F.avg("adjusted_last_known_event_seq"), 2).alias("avg_adjusted_last_known_event_seq"),
        F.min("event_seq_2").alias("min_event_seq_2"),
    )
    .withColumn("min_event_seq_2_percent_rank", F.percent_rank().over(w7))
)
