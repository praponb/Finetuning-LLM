from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(F.year("delivered_date") == 2024)

shipments_stats = (
    recent_shipments
    .groupBy("status", "carrier")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    shipments_stats
    .select("status", "status_values")
    .orderBy("status_values", "status")
)
