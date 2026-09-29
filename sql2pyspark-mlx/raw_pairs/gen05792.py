from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.count("amount").alias("amount_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
    .filter(F.col("amount_count") < 9)
    .select("order_id", "amount_count", "method_values")
)
