from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([8, 9, 10]))
    .groupBy(
        F.when(F.col("status").isin(["RETURNED", "CANCELLED"]), "priority").otherwise("other").alias("status_group"),
        "status",
    )
    .agg(
        F.sum(F.when(F.col("channel") == "store", F.col("amount")).otherwise(0)).alias("store_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
    .filter(F.col("store_amount") > 59)
    .orderBy(F.col("store_amount").desc())
)
