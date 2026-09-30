from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") >= "2023-09-24")
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .filter(F.col("stddev_amount") > 21)
    .select("status", "method_list", "stddev_amount")
    .orderBy(F.col("stddev_amount").desc())
)
