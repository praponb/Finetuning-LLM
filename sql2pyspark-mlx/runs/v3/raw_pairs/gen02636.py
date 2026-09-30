from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 7)
    .groupBy("status", "order_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("avg_amount") >= 22)
    .select("status", "order_id", "avg_amount", "status_values")
    .orderBy("status", "order_id")
    .limit(5)
)
