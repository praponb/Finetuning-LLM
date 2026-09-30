from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy(F.col("avg_cost").desc())
)
