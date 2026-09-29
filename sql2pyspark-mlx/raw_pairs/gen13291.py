from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("merchant", "txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("is_7_eleven", "id")
w3 = Window.partitionBy("txn_type", "account_id").orderBy("next_amount", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("account_id").orderBy("prev_is_7_eleven", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.year("txn_time")).orderBy(F.col("next_amount").desc())
w6 = Window.partitionBy("account_id").orderBy(F.col("prev_is_7_eleven").desc(), "id")

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("txn_time") >= "2025-08-01"))
    .withColumn("is_7_eleven", F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("NONE")))
)

scored = (
    base
    .withColumn("prev_is_7_eleven", F.lag("is_7_eleven").over(w_grp))
    .withColumn("next_is_7_eleven", F.lead("is_7_eleven").over(w_grp))
    .withColumn("next_amount", F.lead("amount").over(w_grp))
    .withColumn("prev_amount", F.lag("amount").over(w_grp))
    .withColumn("seq_no", F.row_number().over(w_grp))
)

deduped = (
    scored
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    deduped
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "is_7_eleven",
        "merchant_filled",
        "prev_amount",
        F.avg("prev_amount").over(w3).alias("prev_amount_avg_5"),
        F.sum("seq_no").over(w4).alias("cumulative_seq_no"),
        F.percent_rank().over(w5).alias("next_amount_percent_rank"),
        F.row_number().over(w6).alias("seq_no_2"),
    )
)

totals = (
    with_windows
    .groupBy("txn_type")
    .agg(
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count"),
        F.sum(F.when(F.col("merchant") == "Grab", F.col("amount"))).alias("grab_total_amount"),
        F.percentile_approx("prev_amount_avg_5", [0.25, 0.5, 0.75]).alias("prev_amount_avg_5_quartiles"),
        F.max(F.when(F.col("merchant") == "Grab", F.col("prev_amount_avg_5"))).alias("grab_max_prev_amount_avg_5"),
    )
    .filter(F.col("grab_max_prev_amount_avg_5") > 0)
)

result = (
    totals
    .select(
        "txn_type",
        "debit_count",
        "grab_total_amount",
        "prev_amount_avg_5_quartiles",
        "grab_max_prev_amount_avg_5",
        F.when(F.col("txn_type").rlike("^(REFUND|CREDIT)$"), "CORE").otherwise("GROUP_B").alias("txn_type_zone"),
    )
)
