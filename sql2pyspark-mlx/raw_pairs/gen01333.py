from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & F.col("amount").between(1200, 1900))
    .select(
        "*",
        F.upper("txn_type").alias("txn_type_upper"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_filled"),
    )
)

filtered = (
    base
    .filter(F.col("txn_time") >= F.to_timestamp(F.lit("2023-08-06 20:00:00")))
    .select(
        "*",
        F.length("txn_type").alias("txn_type_length"),
        F.when(F.col("amount") >= 4200, "High").when(F.col("amount") >= 1200, "Medium").otherwise("Low").alias("amount_level"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "txn_type_upper",
        "merchant_filled",
        "txn_type_length",
        F.count("amount").over(w).alias("running_count_amount"),
        F.min("amount").over(w).alias("running_min_amount"),
        F.sum("amount").over(w).alias("running_sum_amount"),
    )
)

valid = (
    enriched
    .filter(F.col("running_min_amount") > 3100)
    .withColumn("merchant_upper", F.upper("merchant"))
    .withColumn("merchant_filled_2", F.coalesce("merchant", F.lit("OTHER")))
)

cleaned = (
    valid
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "merchant_filled",
        "running_count_amount",
        "running_min_amount",
        "running_sum_amount",
        "merchant_upper",
        "merchant_filled_2",
        F.when(F.col("running_count_amount") >= 18, "High").when(F.col("running_count_amount") >= 7, "Medium").otherwise("Low").alias("running_count_amount_level"),
        F.length("merchant").alias("merchant_length"),
    )
)

valid_transactions = (
    cleaned
    .filter(F.col("merchant_length") == 49)
    .select("*", F.length("merchant").alias("merchant_length_2"))
)

result = (
    valid_transactions
    .select(
        "id",
        "amount",
        "merchant",
        "is_flagged",
        "running_count_amount",
        "running_min_amount",
        "running_sum_amount",
        "merchant_upper",
        "merchant_filled_2",
    )
)
