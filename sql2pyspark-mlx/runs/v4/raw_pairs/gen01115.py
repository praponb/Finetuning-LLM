from pyspark.sql import functions as F

result = (
    df_payments
    .withColumn("quarter_start", F.date_trunc("QUARTER", "paid_at"))
    .withColumn("amount_tier", F.when(F.col("amount") >= 1470, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["quarter_start", "amount_tier"], ["quarter_start"]],
        "quarter_start",
        "amount_tier",
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.grouping("quarter_start").alias("grouping_id"),
    )
)
