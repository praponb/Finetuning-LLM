from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id")
w2 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc_nulls_last(), "id")

cleaned = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-06-14 06:00:00").cast("timestamp"))
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)

enriched = (
    cleaned
    .withColumn("next_amount", F.lead("amount", 2).over(w))
    .withColumn("amount_count_30d", F.count("amount").over(w2))
    .withColumn("amount_count_30d_2", F.count("amount").over(w3))
    .withColumn("last_known_merchant", F.last("merchant", ignorenulls=True).over(w4))
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

cleaned_transactions = (
    deduped
    .filter(F.col("txn_time") > F.lit("2024-01-03").cast("date"))
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "amount_count_30d",
        "amount_count_30d_2",
        "last_known_merchant",
        "latest_rank",
        F.when(F.col("amount") >= 3100, "High").when(F.col("amount") >= 1700, "Medium").otherwise("Low").alias("amount_band"),
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_clean"),
    )
)

result = (
    cleaned_transactions
    .select(
        "id",
        "txn_time",
        "merchant",
        "amount_count_30d",
        "amount_count_30d_2",
        "last_known_merchant",
        "latest_rank",
        "amount_band",
    )
)
