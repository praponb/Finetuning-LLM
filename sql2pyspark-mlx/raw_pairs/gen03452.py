from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status", "carrier")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sum("cost").alias("total_cost"),
    )
    .orderBy("status_values", "status", "carrier")
    .limit(20)
)
