from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(59, 80))
    .agg(
        F.max("shipped_date").alias("latest_shipped_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy("latest_shipped_date")
)
