from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("txn_time", "id")
w2 = Window.orderBy("greatest_amount_int_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("next_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(~F.col("is_flagged") & (F.col("merchant").eqNullSafe(F.lit("Grab"))))
    .select("*", F.substring("txn_type", 1, 4).alias("txn_type_prefix"))
)

filtered = (
    valid
    .filter((F.col("txn_time") >= F.lit("2023-09-24").cast("date")) & (F.col("merchant").eqNullSafe(F.lit("7-Eleven"))))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
    .withColumn("amount_int", F.col("amount").cast("int"))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("N/A")))
)

base = (
    filtered
    .filter((F.col("txn_time") >= F.lit("2023-09-25").cast("date")) & (F.col("merchant").eqNullSafe(F.lit("Shell"))))
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "txn_type_prefix",
        "is_flagged_label",
        "amount_int",
        "merchant_filled",
        F.when(F.col("txn_type") == "FEE", 1).otherwise(0).alias("is_fee"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_filled_2"),
    )
)

valid_transactions = (
    base
    .filter(F.col("txn_time") > F.lit("2023-07-30 22:00:00").cast("timestamp"))
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "txn_type_prefix",
        "is_flagged_label",
        "merchant_filled",
        "merchant_filled_2",
        F.greatest("amount_int", "amount").alias("greatest_amount_int_amount"),
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label_2"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_clean"),
    )
)

filtered_transactions = (
    valid_transactions
    .filter((F.col("merchant") == "7-Eleven") & F.col("merchant").isNotNull())
    .select(
        "id",
        "amount",
        "txn_time",
        "txn_type_prefix",
        "merchant_filled_2",
        "greatest_amount_int_amount",
        "merchant_clean",
        F.length("merchant").alias("merchant_length"),
        (F.col("greatest_amount_int_amount") * F.col("amount")).alias("greatest_amount_int_amount_times_amount"),
        F.coalesce("merchant", F.lit("OTHER")).alias("resolved_merchant"),
    )
)

enriched = (
    filtered_transactions
    .filter(F.col("amount").between(1500, 2200))
    .select(
        "*",
        F.lead("amount").over(w_hist).alias("next_amount"),
        F.lead("merchant_length").over(w_hist).alias("next_merchant_length"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.last("merchant_length", ignorenulls=True).over(w2).alias("last_known_merchant_length"),
    )
)

result = (
    enriched
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant_clean",
        "merchant_length",
        "resolved_merchant",
        "next_merchant_length",
        "event_seq",
        "last_known_merchant_length",
        F.sum("merchant_length").over(w3).alias("running_sum_merchant_length"),
        F.count("merchant_length").over(w3).alias("running_count_merchant_length"),
        F.avg("merchant_length").over(w3).alias("running_avg_merchant_length"),
        F.max("merchant_length").over(w3).alias("running_max_merchant_length"),
    )
)
