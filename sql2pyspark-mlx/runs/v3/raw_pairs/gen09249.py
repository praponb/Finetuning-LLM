from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") <= 0.17)
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
    .orderBy(F.col("channel_values").desc(), "status")
    .limit(10)
)
