from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter(F.year("shipped_date") >= 2024)
    .withColumn("days_since_shipped_date", F.datediff(F.current_date(), "shipped_date"))
)

shipments_stats = (
    valid_shipments
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
    )
)

result = (
    shipments_stats
    .select("status", "carrier_values")
    .orderBy("carrier_values")
)
