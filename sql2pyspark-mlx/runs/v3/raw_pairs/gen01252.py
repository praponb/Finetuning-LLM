from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.sum("cost").alias("total_cost"),
    )
    .orderBy(F.col("status").desc())
)
