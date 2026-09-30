from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 4300, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("txn_type") == "FEE", 1)), F.lit(0)), 2).alias("amount_per_fee"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
    .orderBy(F.col("amount_stddev").desc_nulls_last())
)
