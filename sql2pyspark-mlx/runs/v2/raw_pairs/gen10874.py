from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull() | (F.col("cost") > 80))
    .groupBy("order_id", "carrier")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
    .filter(F.col("cost_range") > 59)
)
