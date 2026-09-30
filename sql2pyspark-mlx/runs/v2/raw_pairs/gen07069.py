from pyspark.sql import functions as F

result = (
    df_orders
    .filter(((F.col("status") == "CANCELLED") & (F.col("status") == "COMPLETED")) | ~F.col("status").isin(["CANCELLED", "COMPLETED"]))
    .cube("channel", "status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
    )
    .orderBy(F.col("total_shipping_fee").desc())
)
