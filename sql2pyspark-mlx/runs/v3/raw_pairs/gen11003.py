from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", "store_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .orderBy(F.col("store_id").desc())
)
