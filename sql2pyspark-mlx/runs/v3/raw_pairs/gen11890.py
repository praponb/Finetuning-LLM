from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(F.col("status") == "IN_TRANSIT")

summary = (
    recent_shipments
    .groupBy("status", "order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
    )
)

result = (
    summary
    .select("status", "order_id", "carrier_values")
    .orderBy(F.col("order_id").desc())
)
