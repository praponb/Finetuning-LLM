from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w_part = Window.partitionBy(F.date_trunc("week", "txn_time"), "merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy("amount", "id")
w5 = Window.partitionBy("status").orderBy(F.col("next_amount").desc())
w6 = Window.partitionBy("account_id").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("merchant").eqNullSafe(F.lit("7-Eleven"))))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.concat_ws("-", "txn_type", "merchant").alias("txn_type_merchant_key"),
    )
)

filtered = (
    cleaned
    .filter(F.col("amount") <= 1600)
    .select(
        "*",
        F.when(F.col("merchant").isin(["Lazada", "Amazon"]), "Core").otherwise("Other").alias("merchant_group"),
    )
)

metrics = (
    filtered
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w_part))
    .withColumn("cumulative_amount", F.sum("amount").over(w_grp))
)

enriched = (
    metrics
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "account_id",
        "merchant",
        "is_flagged",
        "merchant_group",
        "seq_no",
        "last_known_amount",
        F.lead("last_known_amount").over(w4).alias("next_last_known_amount"),
        F.lag("amount").over(w4).alias("prev_amount"),
        F.lead("amount").over(w4).alias("next_amount"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

cleaned_transactions = (
    enriched
    .filter(F.col("merchant").isNotNull() & (F.col("last_known_amount") > 3800))
    .select(
        "*",
        F.when(F.col("next_amount") >= 4000, "High").when(F.col("next_amount") >= 1200, "Medium").otherwise("Low").alias("next_amount_tier"),
        F.length("merchant").alias("merchant_length"),
    )
)

with_dims = (
    cleaned_transactions
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.account_id",
        "e.seq_no",
        "e.last_known_amount",
        "e.next_last_known_amount",
        "e.next_amount",
        "e.event_seq",
        "e.next_amount_tier",
        "e.merchant_length",
        F.col("b.status"),
    )
)

result = (
    with_dims
    .select(
        "id",
        "account_id",
        "last_known_amount",
        "next_last_known_amount",
        "next_amount",
        "merchant_length",
        "status",
        F.percent_rank().over(w5).alias("next_amount_percent_rank"),
        F.sum("last_known_amount").over(w6).alias("running_last_known_amount"),
    )
)
