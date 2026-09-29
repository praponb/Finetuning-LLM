from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.month("paid_at").alias("paid_month"), "method")
    .agg(
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
    .orderBy(F.col("method_values").desc(), "paid_month")
)
