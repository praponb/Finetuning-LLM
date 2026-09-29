from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2023-04-01")
    .groupBy(
        F.when(F.col("amount") >= 4000, "high").when(F.col("amount") >= 1900, "mid").otherwise("low").alias("amount_bucket"),
        "is_flagged",
    )
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
    .filter(F.col("amount_range") >= 62)
    .select("amount_bucket", "is_flagged", "amount_range", "txn_type_list")
)
