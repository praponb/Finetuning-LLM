from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 39)
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy(F.col("method_values").desc())
)
