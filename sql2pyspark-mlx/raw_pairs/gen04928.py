from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-09-22").cast("date")) & (~F.col("merchant").eqNullSafe(F.lit("Grab"))))
    .withColumn("txn_time_label", F.date_format("txn_time", "MMM yyyy"))
)

base = (
    filtered
    .filter((F.col("txn_time") >= F.to_timestamp(F.lit("2023-11-03 22:00:00"))) & (F.col("merchant").eqNullSafe(F.lit("Lazada"))))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "txn_time_label",
        F.ceil("amount").alias("amount_whole"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_filled"),
    )
)

with_gaps = (
    base
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount_whole",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

base_transactions = (
    islands
    .filter(F.col("txn_type") == "CREDIT")
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount_whole",
        (F.col("amount_whole") - F.col("island_id")).alias("amount_whole_minus_island_id"),
    )
)

result = (
    base_transactions
    .filter(F.to_date("txn_time") < "2024-03-04")
    .select(
        "id",
        "txn_type",
        "txn_time",
        F.lead("amount_whole").over(w).alias("next_amount_whole"),
        F.lag("amount_whole").over(w).alias("prev_amount_whole"),
        F.lead("amount_whole_minus_island_id").over(w).alias("next_amount_whole_minus_island_id"),
        F.lag("amount_whole_minus_island_id").over(w).alias("prev_amount_whole_minus_island_id"),
        F.row_number().over(w).alias("seq_no"),
    )
)
