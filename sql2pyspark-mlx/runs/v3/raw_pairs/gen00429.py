from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter(F.col("discount") >= 0.35)
    .withColumn("discount_rounded", F.round("discount", 2))
)

summary = (
    filtered_orders
    .groupBy("store_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    summary
    .select("store_id", "status_values")
    .orderBy("status_values")
)
