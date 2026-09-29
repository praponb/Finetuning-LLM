from pyspark.sql import functions as F

result = (
    df_payments
    .withColumn("week_start", F.date_trunc("WEEK", "paid_at"))
    .withColumn("amount_tier", F.when(F.col("amount") >= 1670, "HIGH").otherwise("LOW"))
    .cube("week_start", "amount_tier")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "FAILED", 1)), F.lit(0)), 2).alias("amount_per_failed"),
        F.count("*").alias("row_count"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("week_start").desc_nulls_last())
)
