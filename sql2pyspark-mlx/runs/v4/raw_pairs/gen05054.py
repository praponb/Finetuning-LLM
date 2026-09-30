from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc_nulls_last(), "id")

cleaned = (
    df_transactions
    .filter(F.col("merchant").isin(["Shell", "Grab", "7-Eleven"]))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.when(F.col("amount") >= 4300, "High").when(F.col("amount") >= 600, "Medium").otherwise("Low").alias("amount_tier"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_filled"),
    )
)

result = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)
