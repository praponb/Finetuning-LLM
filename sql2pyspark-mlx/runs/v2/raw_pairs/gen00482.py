from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") > 27)
    .groupBy("channel")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy("status_values")
)
