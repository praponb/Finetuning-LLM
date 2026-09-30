from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy("txn_type_length", "id")
w2 = Window.partitionBy("merchant", "txn_type").orderBy("amount", "id")
w3 = Window.orderBy(F.col("txn_type_length").desc())
w4 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.partitionBy("merchant").orderBy(F.col("txn_type_length_percent_rank").desc_nulls_last(), "id")

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "*",
        F.length("txn_type").alias("txn_type_length"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_filled"),
    )
)

latest = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.ntile(4).over(w2).alias("amount_bucket"),
        F.percent_rank().over(w3).alias("txn_type_length_percent_rank"),
        F.sum("amount").over(w4).alias("amount_sum_7d"),
    )
)

latest_transactions = (
    scored
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

result = (
    latest_transactions
    .select(
        "id",
        "amount",
        "merchant",
        "txn_type_length",
        "merchant_filled",
        "dedup_rank",
        "txn_type_length_percent_rank",
        "amount_sum_7d",
        F.when(F.col("merchant").rlike("^(Amazon|Lazada)$"), "CORE").when(F.col("merchant").rlike("^(Grab|7-Eleven)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("merchant_zone"),
    )
)
