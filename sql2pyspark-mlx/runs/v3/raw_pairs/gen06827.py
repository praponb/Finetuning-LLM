from pyspark.sql import functions as F

src = (
    df_orders
    .filter(F.col("amount") > 1330)
    .withColumn("discount_minus_amount", (F.col("discount") - F.col("amount")))
)

status_summary = (
    src
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", F.col("discount_minus_amount")).otherwise(0)).alias("returned_discount_minus_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
)

result = (
    status_summary
    .orderBy(F.col("status").desc(), "channel_values")
    .limit(20)
)
