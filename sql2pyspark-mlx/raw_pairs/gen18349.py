from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")

filtered = (
    df_transactions
    .filter((F.col("txn_time") >= F.to_timestamp(F.lit("2023-05-08 16:00:00"))) & F.col("account_id").isin([11, 20, 22]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.trim("merchant").alias("clean_merchant"),
        F.concat_ws("-", "txn_type", "merchant").alias("txn_type_merchant_key"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "account_id", "txn_type", "clean_merchant", "txn_type_merchant_key")
)

filtered_transactions = (
    latest
    .filter(F.col("account_id").isin([12, 16]))
    .select("*", F.substring("txn_type", 1, 2).alias("txn_type_prefix"))
)

enriched = (
    filtered_transactions
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .filter(F.year(F.col("a.opened_date")) >= 2024)
    .select("s.*", F.col("a.account_type"), F.col("a.balance"), F.col("a.opened_date"))
)

result = (
    enriched
    .groupBy("txn_type", "account_id")
    .pivot("account_type", ["LOAN", "CHECKING"])
    .agg(F.max("balance").alias("amount"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "txn_type",
        "account_id",
        F.col("LOAN_amount").alias("loan_amount"),
        F.col("LOAN_item_count").alias("loan_item_count"),
        F.col("CHECKING_amount").alias("checking_amount"),
        F.col("CHECKING_item_count").alias("checking_item_count"),
    )
)
