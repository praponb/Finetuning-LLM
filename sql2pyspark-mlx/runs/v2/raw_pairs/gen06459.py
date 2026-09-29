from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(710, 1570))
    .groupBy("status")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
    )
    .orderBy(F.col("success_count").desc())
)
