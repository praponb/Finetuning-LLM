from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter(F.month("shipped_date").isin([1, 4, 6]) & (F.col("shipped_date") < "2024-05-29"))
    .withColumn("cost_filled", F.coalesce("cost", F.lit(0)))
)

order_id_summary = (
    valid_shipments
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = order_id_summary.select("order_id", "status_values")
