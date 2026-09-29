from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") >= "2023-07-01") | F.col("order_id").isin([12, 14, 15, 36]))
    .groupBy(
        F.when(F.col("status").isin(["SUCCESS", "REFUNDED"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.min("amount").alias("min_amount"),
    )
)
