from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status").orderBy("opened_date", "id")
w3 = Window.partitionBy("account_type", "status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_type").orderBy(F.col("avg_next_is_closed").desc())

cleaned = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-08-18").cast("date")) & F.col("status").isNotNull())
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.concat_ws("-", "status", "account_type").alias("status_account_type_key"),
        F.when(F.col("status") == "CLOSED", 1).otherwise(0).alias("is_closed"),
        F.coalesce("status", F.lit("OTHER")).alias("status_filled"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant").alias("transaction_merchant"))
)

scored = (
    with_last
    .select(
        "id",
        "account_type",
        "status_account_type_key",
        "is_closed",
        "status_filled",
        "transaction_merchant",
        F.lead("is_closed", 1).over(w2).alias("next_is_closed"),
        F.sum("balance").over(w3).alias("running_balance"),
    )
)

filtered = (
    scored
    .filter(F.col("transaction_merchant").isNotNull() & (F.col("transaction_merchant").eqNullSafe(F.lit("Grab"))))
    .select(
        "id",
        "account_type",
        "status_account_type_key",
        "status_filled",
        "transaction_merchant",
        "next_is_closed",
        F.least("is_closed", "next_is_closed").alias("least_is_closed_next_is_closed"),
        F.coalesce("transaction_merchant", F.lit("DIRECT")).alias("resolved_transaction_merchant"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (~F.col("c.is_flagged")), "left_semi")
)

result = (
    not_flagged
    .groupBy(
        "account_type",
        F.when(F.col("least_is_closed_next_is_closed") >= 76, "PREMIUM").otherwise("STANDARD").alias("least_is_closed_next_is_closed_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("least_is_closed_next_is_closed", [0.25, 0.5, 0.75]).alias("least_is_closed_next_is_closed_quartiles"),
        F.round(F.avg("next_is_closed"), 2).alias("avg_next_is_closed"),
    )
    .withColumn("avg_next_is_closed_dense_rank", F.dense_rank().over(w4))
)
