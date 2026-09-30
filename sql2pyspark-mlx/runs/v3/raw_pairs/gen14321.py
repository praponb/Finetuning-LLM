from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") > "2023-09-01")
    .groupBy("status")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("avg_cost") >= 52)
    .select("status", "avg_cost", "status_list")
    .orderBy(F.col("avg_cost").desc())
)
