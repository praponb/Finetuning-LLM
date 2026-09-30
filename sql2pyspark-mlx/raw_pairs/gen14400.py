from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.date_trunc("YEAR", "txn_time").alias("year_start"),
        F.date_trunc("MONTH", "txn_time").alias("activity_month"),
        F.when(F.col("amount") >= 3800, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("txn_type") == "FEE", 1)), F.lit(0)), 2).alias("amount_per_fee"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_rows"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("amount")).otherwise(0)).alias("fee_amount"),
        F.min("txn_time").alias("first_seen"),
    )
    .orderBy("amount_per_fee")
)
