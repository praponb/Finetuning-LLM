from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        F.sum(F.when(F.col("channel") == "store", F.col("amount")).otherwise(0)).alias("store_amount"),
    )
    .filter(F.col("store_amount") >= 20)
)
