from pyspark.sql import functions as F

result = (
    df_payments
    .withColumn("week_start", F.date_trunc("WEEK", "paid_at"))
    .rollup("week_start", "method")
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount"))).alias("success_total_amount"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping("week_start").alias("grouping_level"),
    )
    .filter(F.col("success_total_amount") > 0)
)
