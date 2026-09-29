from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id")
w2 = Window.partitionBy("account_id")
w3 = Window.partitionBy("txn_type").orderBy(F.col("event_seq").desc(), "id")
w4 = Window.partitionBy("account_id", "status").orderBy("account_id_avg_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter((F.col("txn_time") > F.to_timestamp(F.lit("2023-10-11 19:00:00"))) & F.col("merchant").isNotNull())
    .select(
        "*",
        F.when(F.col("amount") >= 4000, "High").when(F.col("amount") >= 1800, "Medium").otherwise("Low").alias("amount_band"),
        F.coalesce("merchant", F.lit("N/A")).alias("resolved_merchant"),
    )
)

metrics = (
    valid
    .withColumn("next_amount", F.lead("amount").over(w))
    .withColumn("prev_amount", F.lag("amount").over(w))
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("account_id_avg_amount", F.avg("amount").over(w2))
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    ranked
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "left")
    .filter(F.col("b.customer_id").isin([4, 12, 36, 37]))
    .select(
        "s.id",
        "s.account_id",
        "s.resolved_merchant",
        "s.event_seq",
        "s.account_id_avg_amount",
        F.col("b.balance"),
        F.col("b.opened_date"),
        F.col("b.status"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.count("account_id_avg_amount").over(w4).alias("running_count_account_id_avg_amount"),
        F.min("account_id_avg_amount").over(w4).alias("running_min_account_id_avg_amount"),
        F.avg("account_id_avg_amount").over(w4).alias("running_avg_account_id_avg_amount"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "status",
        F.col("balance").alias("BALANCE_VAL"),
        F.col("running_avg_account_id_avg_amount").alias("RUNNING_AVG_ACCOUNT_ID_AVG_AMOUNT"),
    )
    .unpivot(
        ids=["id", "status"],
        values=["BALANCE_VAL", "RUNNING_AVG_ACCOUNT_ID_AVG_AMOUNT"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
