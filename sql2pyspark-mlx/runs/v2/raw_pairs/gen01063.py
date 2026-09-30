from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
    .filter(F.col("avg_amount") >= 64)
    .select("order_id", "avg_amount", "method_values")
    .orderBy(F.col("order_id").desc())
)
