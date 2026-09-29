from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") > 1520) & F.col("status").isin(["FAILED", "REFUNDED"]))
    .groupBy("status", "method")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.min("amount").alias("min_amount"),
    )
)
