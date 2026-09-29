from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter((F.col("order_id") == 26) & F.col("carrier").isin(["Flash", "UPS", "Kerry"]))
)

status_summary = (
    valid_shipments
    .groupBy("status", "order_id")
    .agg(
        F.max("cost").alias("max_cost"),
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
    )
)

result = (
    status_summary
    .filter(F.col("max_cost") > 29)
    .select("status", "order_id", "max_cost", "carrier_values")
    .orderBy("carrier_values", "status", "order_id")
    .limit(50)
)
