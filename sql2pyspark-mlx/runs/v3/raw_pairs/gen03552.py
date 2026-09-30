from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.countDistinct("cost").alias("unique_statuss"),
    )
    .orderBy(F.col("total_cost").desc(), "unique_statuss", "carrier")
    .limit(5)
)
