from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.month("delivered_date").alias("delivered_month"), "status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)
