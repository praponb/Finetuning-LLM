from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(-5, -1)
w_recent = Window.partitionBy("txn_type").orderBy(F.col("txn_time_year").asc_nulls_last())
w3 = Window.partitionBy("txn_type").orderBy(F.col("amount_avg_5_prior").desc(), "id")

cleaned = (
    df_transactions
    .filter((F.col("txn_time") >= F.to_date(F.lit("2023-11-12"))) & (~F.col("merchant").eqNullSafe(F.lit("7-Eleven"))))
    .select("id", "txn_type", "amount", F.year("txn_time").alias("txn_time_year"))
)

enriched = (
    cleaned
    .filter(F.col("amount") >= 1100)
    .select(
        "id",
        "txn_type",
        F.avg("amount").over(w).alias("amount_avg_5_prior"),
        F.dense_rank().over(w_recent).alias("txn_time_year_dense_rank"),
    )
)

valid = (
    enriched
    .filter(F.col("txn_type").isin(["CREDIT", "FEE", "DEBIT"]))
    .withColumn("amount_avg_5_prior_scaled", F.round((F.col("amount_avg_5_prior") * 1.15), 2))
    .withColumn("clean_txn_type", F.trim("txn_type"))
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

filtered = (
    deduped
    .filter(F.col("amount_avg_5_prior_scaled") <= 29)
    .select(
        "id",
        "txn_type",
        "txn_time_year_dense_rank",
        "amount_avg_5_prior_scaled",
        "latest_rank",
        F.when(F.col("txn_time_year_dense_rank") >= 8, "High").when(F.col("txn_time_year_dense_rank") >= 3, "Medium").otherwise("Low").alias("txn_time_year_dense_rank_tier"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("txn_time_year_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("txn_time_year_dense_rank_tier"),
        F.when(F.col("amount_avg_5_prior_scaled") >= 78, "LARGE").otherwise("SMALL").alias("amount_avg_5_prior_scaled_tier"),
    )
    .agg(
        F.avg(F.when(F.col("txn_type") == "CREDIT", F.col("amount_avg_5_prior_scaled"))).alias("credit_avg_amount_avg_5_prior_scaled"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.count_if(F.col("txn_type") == "REFUND").alias("refund_events"),
        F.count_if(F.col("txn_type") == "DEBIT").alias("debit_events"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("amount_avg_5_prior_scaled")).otherwise(0)).alias("fee_amount_avg_5_prior_scaled"),
    )
    .orderBy(F.col("amount_avg_5_prior_scaled_tier").desc())
)
