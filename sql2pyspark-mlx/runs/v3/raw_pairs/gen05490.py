from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNull() & (F.col("discount") > 0.36) & F.col("amount").between(1000, 1540))
    .groupBy("channel", "status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
)
