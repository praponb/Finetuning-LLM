from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id") == 35)
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
    )
    .filter(F.col("cancelled_count") < 70)
)
