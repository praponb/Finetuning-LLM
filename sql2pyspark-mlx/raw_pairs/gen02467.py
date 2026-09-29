from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "merchant",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.initcap(F.trim("merchant")).alias("merchant_title"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_clean"),
    )
)

valid = (
    base
    .filter(F.col("merchant").isNotNull() & F.col("txn_type").isin(["CREDIT", "FEE"]))
    .select(
        "*",
        F.upper("merchant").alias("merchant_upper"),
        F.concat_ws("-", "txn_type", "merchant").alias("txn_type_merchant_key"),
    )
)

cleaned = (
    valid
    .filter(F.col("merchant").isNotNull() & F.col("txn_type").isin(["CREDIT", "DEBIT", "FEE"]))
    .select("*", F.upper("merchant").alias("merchant_upper_2"))
)

filtered = (
    cleaned
    .filter(F.col("merchant").eqNullSafe(F.lit("Grab")))
    .select(
        "id",
        "txn_type",
        "merchant",
        "is_flagged_label",
        "merchant_title",
        "merchant_clean",
        "merchant_upper",
        "merchant_upper_2",
        F.trim("merchant").alias("clean_merchant"),
    )
)

filtered_transactions = (
    filtered
    .filter(F.col("merchant").isin(["Grab", "Lazada", "Shell"]))
    .select(
        "id",
        "txn_type",
        "merchant",
        "is_flagged_label",
        "merchant_title",
        "merchant_clean",
        "merchant_upper",
        "merchant_upper_2",
        "clean_merchant",
        F.substring("merchant", 1, 4).alias("merchant_prefix"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_clean_2"),
    )
)

base_transactions = (
    filtered_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "merchant",
        "is_flagged_label",
        "merchant_title",
        "merchant_clean",
        "merchant_upper",
        "merchant_upper_2",
        "merchant_clean_2",
        F.length("txn_type").alias("txn_type_length"),
        F.lower("txn_type").alias("txn_type_lower"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_filled"),
    )
)

result = (
    base_transactions
    .select(
        "id",
        "txn_type",
        "merchant",
        "is_flagged_label",
        "merchant_title",
        "merchant_upper",
        "merchant_upper_2",
        "merchant_filled",
    )
)
