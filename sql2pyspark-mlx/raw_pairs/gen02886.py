from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status", F.year("delivered_date").alias("delivered_year"))
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy(F.col("avg_cost").desc())
)
