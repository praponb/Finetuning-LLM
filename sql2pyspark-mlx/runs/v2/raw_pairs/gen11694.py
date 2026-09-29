from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull() & F.col("order_id").isin([4, 12]) & F.col("order_id").isin([19, 27, 40]))
    .rollup("status", "method")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy(F.col("method").desc())
)
