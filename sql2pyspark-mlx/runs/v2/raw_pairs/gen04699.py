from pyspark.sql import functions as F

src = (
    df_payments
    .filter((F.col("amount") >= 1420) & (F.col("status") == "REFUNDED") & F.col("order_id").isNotNull())
)

summary = (
    src
    .groupBy("method")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    summary
    .filter(F.col("avg_amount") < 32)
    .orderBy(F.col("method").desc(), F.col("avg_amount").desc())
)
