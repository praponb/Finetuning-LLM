from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.count("*").alias("num_shipments"),
    )
    .filter(F.col("num_shipments") < 1)
)
