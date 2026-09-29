from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status", "method")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("total_amount") > 7400)
)
