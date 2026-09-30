from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("merchant", "txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant", "account_id").orderBy("amount_avg_3", "id")

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2024-04-06 14:00:00").cast("timestamp")) & F.col("merchant").isNotNull() & F.col("amount").between(1400, 1900))
    .select(
        "*",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.coalesce("merchant", F.lit("N/A")).alias("resolved_merchant"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.avg("amount").over(w).alias("amount_avg_3"),
        F.ntile(4).over(w2).alias("amount_bucket"),
        F.last("merchant", ignorenulls=True).over(w3).alias("last_known_merchant"),
    )
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

cleaned_transactions = (
    latest
    .filter((F.col("txn_time") >= F.to_date(F.lit("2023-08-26"))) & F.col("merchant").isNotNull())
    .select(
        "*",
        F.greatest("amount", "amount_bucket").alias("greatest_amount_amount_bucket"),
        F.lower("merchant").alias("merchant_lower"),
    )
)

result = (
    cleaned_transactions
    .select(
        "id",
        "merchant",
        F.col("greatest_amount_amount_bucket").alias("GREATEST_AMOUNT_AMOUNT_BUCKET"),
        F.col("amount").alias("AMOUNT"),
    )
    .unpivot(
        ["id", "merchant"],
        ["GREATEST_AMOUNT_AMOUNT_BUCKET", "AMOUNT"],
        "metric",
        "reading",
    )
)
