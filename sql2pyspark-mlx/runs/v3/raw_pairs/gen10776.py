from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1470, "high").when(F.col("amount") >= 790, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("amount")).otherwise(0)).alias("refunded_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("stddev_amount") >= 46)
)
